// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.IO;
using System.IO.Pipes;
using System.Management.Automation.Language;
using System.Management.Automation.Runspaces;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;

namespace Microsoft.PowerShell
{
    internal sealed partial class ConsoleHost
    {
        /// <summary>
        /// Out-of-band shell event channel. When the host did not inject the
        /// endpoint/token environment variables, every write is a no-op.
        /// </summary>
        private static readonly ConsoleHostShellEventChannel s_shellEvents =
            ConsoleHostShellEventChannel.CreateFromEnvironment();

        /// <summary>
        /// Send a shell-integration event out of band. Never writes OSC to stdout:
        /// arbitrary child processes must not be able to forge command boundaries.
        /// </summary>
        private static void WriteShellEvent(
            string kind,
            int? code = null,
            string command = null,
            long? startedAtMs = null,
            long? endedAtMs = null)
        {
            s_shellEvents.Write(kind, code, command, startedAtMs, endedAtMs);
        }

        /// <summary>
        /// Command-boundary bookkeeping for the out-of-band event channel.
        /// Lives here (not in ConsoleHost.cs) so upstream merges only see two
        /// short call sites inside the input loop.
        /// </summary>
        private sealed partial class InputLoop
        {
            /// <summary>
            /// Emit the command-start boundary, or cancel the pending record for
            /// input that will not execute. Incomplete input is continued by the
            /// prompt loop, so it must not start a command.
            /// </summary>
            /// <param name="line">Input line about to be executed.</param>
            /// <returns>True when a command started and D should be emitted later.</returns>
            private static bool BeforeCommand(string line)
            {
                if (HasIncompleteParse(line))
                {
                    WriteShellEvent("X");
                    return false;
                }

                WriteShellEvent(
                    "C",
                    command: line,
                    startedAtMs: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                return true;
            }

            /// <summary>
            /// Emit the command-end boundary with the resolved exit code. Skipped
            /// when no command started, or when execution still reports an
            /// incomplete parse (the command never ran).
            /// </summary>
            /// <param name="commandStarted">Result returned by <see cref="BeforeCommand"/>.</param>
            /// <param name="e">Execution exception, or null on success.</param>
            private void AfterCommand(bool commandStarted, Exception e)
            {
                if (!commandStarted || (e != null && IsIncompleteParseException(e)))
                {
                    return;
                }

                // The host's output writer can still hold buffered output when
                // execution returns. Flush content already produced so D is
                // queued after the command's visible output, not before it.
                try
                {
                    Console.Out.Flush();
                    Console.Error.Flush();
                }
                catch
                {
                    // Redirected/closed console streams: events still work.
                }

                WriteShellEvent(
                    "D",
                    GetCommandExitCode(e),
                    endedAtMs: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            }

            /// <summary>
            /// Resolve the exit code for the command that just completed.
            /// $? is authoritative: a successful command reports 0 even when a
            /// previous native command left a sticky LASTEXITCODE. Only a failed
            /// command falls back to LASTEXITCODE.
            /// </summary>
            /// <param name="exceptionThrown">Execution exception, or null on success.</param>
            /// <returns>Process-style exit code for the completed command.</returns>
            private int GetCommandExitCode(Exception exceptionThrown)
            {
                if (exceptionThrown != null)
                {
                    return 1;
                }

                try
                {
                    Runspace currentRunspace = _parent.Runspace;
                    if (currentRunspace == null)
                    {
                        return 0;
                    }

                    object success = currentRunspace.SessionStateProxy.GetVariable("?");
                    if (!(success is bool succeeded) || succeeded)
                    {
                        return 0;
                    }

                    object lastExitCode = currentRunspace.SessionStateProxy.GetVariable("LASTEXITCODE");
                    if (lastExitCode is int intCode)
                    {
                        return intCode;
                    }

                    if (lastExitCode is long longCode
                        && longCode >= int.MinValue
                        && longCode <= int.MaxValue)
                    {
                        return (int)longCode;
                    }

                    return 1;
                }
                catch
                {
                    // The runspace may have been closed by 'exit'; fall back to success.
                }

                return 0;
            }

            /// <summary>
            /// Parse-only incomplete detection used before executing a command.
            /// </summary>
            /// <param name="line">Input line about to be executed.</param>
            /// <returns>True when the line parses as incomplete input.</returns>
            private static bool HasIncompleteParse(string line)
            {
                Parser.ParseInput(line, out _, out ParseError[] errors);
                foreach (ParseError error in errors)
                {
                    if (error.IncompleteInput)
                    {
                        return true;
                    }
                }

                return false;
            }
        }
    }

    /// <summary>
    /// Bounded, non-blocking event writer for the termx out-of-band channel.
    /// A dedicated background thread owns the connection; prompt handling only
    /// does a non-blocking TryAdd and never waits for I/O.
    /// </summary>
    internal sealed class ConsoleHostShellEventChannel
    {
        private const string EndpointVariable = "TERMX_EVENT_ENDPOINT";
        private const string TokenVariable = "TERMX_EVENT_TOKEN";
        private const string PipePrefix = "pipe:";
        private const string UnixPrefix = "unix:";
        private const int MaxQueuedEvents = 64;
        private const int ConnectTimeoutMilliseconds = 3000;

        private readonly BlockingCollection<string> _queue =
            new BlockingCollection<string>(new ConcurrentQueue<string>(), MaxQueuedEvents);

        private readonly string _endpoint;
        private readonly string _token;
        private volatile bool _enabled;
        private string _currentCommandId;

        private ConsoleHostShellEventChannel(string endpoint, string token)
        {
            _endpoint = endpoint;
            _token = token;
            _enabled = !string.IsNullOrEmpty(endpoint) && !string.IsNullOrEmpty(token);
            if (!_enabled)
            {
                return;
            }

            Thread worker = new Thread(Run)
            {
                IsBackground = true,
                Name = "termx-shell-events",
            };
            worker.Start();
        }

        /// <summary>
        /// Read and immediately clear the endpoint/token so child processes
        /// started after this point cannot inherit the token.
        /// </summary>
        internal static ConsoleHostShellEventChannel CreateFromEnvironment()
        {
            string endpoint = Environment.GetEnvironmentVariable(EndpointVariable);
            string token = Environment.GetEnvironmentVariable(TokenVariable);
            Environment.SetEnvironmentVariable(EndpointVariable, null);
            Environment.SetEnvironmentVariable(TokenVariable, null);
            return new ConsoleHostShellEventChannel(endpoint, token);
        }

        internal void Write(
            string kind,
            int? code = null,
            string command = null,
            long? startedAtMs = null,
            long? endedAtMs = null)
        {
            if (!_enabled)
            {
                return;
            }

            string line;
            switch (kind)
            {
                case "C":
                    _currentCommandId = Guid.NewGuid().ToString("N");
                    line = BuildLine(kind, code, _currentCommandId, command, startedAtMs, null);
                    break;
                case "D":
                {
                    string commandId = _currentCommandId;
                    _currentCommandId = null;
                    line = BuildLine(kind, code, commandId, null, null, endedAtMs);
                    break;
                }
                default:
                    line = BuildLine(kind, code, null, null, null, null);
                    break;
            }

            try
            {
                // Queue full: drop the event rather than blocking the prompt.
                _queue.TryAdd(line);
            }
            catch (InvalidOperationException)
            {
                // Channel already closed; keep the prompt usable.
            }
        }

        private void Run()
        {
            try
            {
                using (Stream stream = Connect(_endpoint))
                using (StreamWriter writer = new StreamWriter(stream, new UTF8Encoding(false))
                {
                    AutoFlush = true,
                    NewLine = "\n",
                })
                {
                    writer.WriteLine(BuildHello(_token));
                    foreach (string line in _queue.GetConsumingEnumerable())
                    {
                        writer.WriteLine(line);
                    }
                }
            }
            catch
            {
                // Connect or write failed: permanently degrade to no-op.
                // Transcript falls back to best-effort; never fake an exit code.
            }
            finally
            {
                _enabled = false;
            }
        }

        private static string BuildHello(string token)
        {
            return $"{{\"v\":1,\"token\":\"{token}\"}}";
        }

        private static string BuildLine(
            string kind,
            int? code,
            string commandId,
            string command,
            long? startedAtMs,
            long? endedAtMs)
        {
            StringBuilder builder = new StringBuilder(128);
            builder.Append("{\"v\":1,\"event\":\"").Append(kind).Append('"');
            if (code.HasValue)
            {
                builder
                    .Append(",\"code\":")
                    .Append(code.Value.ToString(CultureInfo.InvariantCulture));
            }

            if (!string.IsNullOrEmpty(commandId))
            {
                builder.Append(",\"id\":").Append(JsonSerializer.Serialize(commandId));
            }

            // 命令文本可能包含引号、反斜杠或换行；必须由 JSON 序列化器转义。
            if (command != null)
            {
                builder.Append(",\"command\":").Append(JsonSerializer.Serialize(command));
            }

            if (startedAtMs.HasValue)
            {
                builder
                    .Append(",\"started_at_ms\":")
                    .Append(startedAtMs.Value.ToString(CultureInfo.InvariantCulture));
            }

            if (endedAtMs.HasValue)
            {
                builder
                    .Append(",\"ended_at_ms\":")
                    .Append(endedAtMs.Value.ToString(CultureInfo.InvariantCulture));
            }

            builder.Append('}');
            return builder.ToString();
        }

        private static Stream Connect(string endpoint)
        {
            if (endpoint.StartsWith(PipePrefix, StringComparison.Ordinal))
            {
                string pipeName = endpoint.Substring(PipePrefix.Length);
                NamedPipeClientStream client = new NamedPipeClientStream(
                    ".",
                    pipeName,
                    PipeDirection.Out,
                    PipeOptions.None);
                client.Connect(ConnectTimeoutMilliseconds);
                return client;
            }

            if (endpoint.StartsWith(UnixPrefix, StringComparison.Ordinal))
            {
#if UNIX
                string socketPath = endpoint.Substring(UnixPrefix.Length);
                Socket socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
                socket.Connect(new UnixDomainSocketEndPoint(socketPath));
                return new NetworkStream(socket, ownsSocket: true);
#else
                throw new PlatformNotSupportedException("Unix event endpoint on Windows.");
#endif
            }

            throw new InvalidOperationException("Unknown shell event endpoint.");
        }
    }
}
