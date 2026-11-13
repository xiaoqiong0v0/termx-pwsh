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
        /// Publish a chunk of command output out of band. Only the active
        /// command id receives output; prompts and input echo have no id.
        /// </summary>
        internal static void WriteShellOutput(string text, bool newLine)
        {
            if (string.IsNullOrEmpty(text))
            {
                return;
            }

            s_shellEvents.WriteOutput(newLine ? text + Environment.NewLine : text);
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

                // Ensure every O frame produced by this command is on the
                // event pipe before D; this is event-queue drain, not a
                // timing-based boundary.
                s_shellEvents.WaitForOutputDrain(2000);

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
        private const int MaxQueuedEvents = 1024;
        private const int MaxOutputChunkChars = 512;
        private const int ConnectTimeoutMilliseconds = 3000;

        private readonly BlockingCollection<string> _queue =
            new BlockingCollection<string>(new ConcurrentQueue<string>(), MaxQueuedEvents);

        private readonly string _endpoint;
        private readonly string _token;
        private readonly object _commandLock = new object();
        private readonly object _outputDrainLock = new object();
        private int _pendingOutput;
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
                {
                    string commandId = Guid.NewGuid().ToString("N");
                    // Build, publish the active id and enqueue under the same
                    // lock as O/D: an O from another thread must never be
                    // queued before this command's C.
                    lock (_commandLock)
                    {
                        _currentCommandId = commandId;
                        TryEnqueue(BuildLine(kind, code, commandId, command, startedAtMs, null));
                    }
                    return;
                }
                case "D":
                {
                    lock (_commandLock)
                    {
                        string commandId = _currentCommandId;
                        _currentCommandId = null;
                        TryEnqueue(BuildLine(kind, code, commandId, null, null, endedAtMs));
                    }
                    return;
                }
                default:
                    line = BuildLine(kind, code, null, null, null, null);
                    break;
            }

            TryEnqueue(line);
        }

        /// <summary>
        /// Non-blocking enqueue shared by C/D/O and other events. A full queue
        /// drops the frame instead of blocking the prompt; a closed channel is
        /// also treated as a drop. C/D call it while holding
        /// <see cref="_commandLock"/> so their order with O is fixed.
        /// </summary>
        /// <param name="line">Encoded JSON Lines frame.</param>
        /// <returns>True when the frame was queued.</returns>
        private bool TryEnqueue(string line)
        {
            try
            {
                return _queue.TryAdd(line);
            }
            catch (InvalidOperationException)
            {
                // Channel already closed; keep the prompt usable.
                return false;
            }
        }

        /// <summary>
        /// Publish command output as out-of-band `O` chunks. No output is
        /// published without an active command id, so prompt/input echo is
        /// excluded by construction.
        /// </summary>
        internal void WriteOutput(string text)
        {
            if (!_enabled || string.IsNullOrEmpty(text))
            {
                return;
            }

            // Hold the same command lock while checking id and enqueueing all
            // chunks. C publishes the id and enqueues under this lock; D
            // clears the id and enqueues under this lock, so O can never be
            // queued before C or after D. A full queue (capacity
            // MaxQueuedEvents) drops the chunk instead of blocking the prompt.
            lock (_commandLock)
            {
                string commandId = _currentCommandId;
                if (string.IsNullOrEmpty(commandId))
                {
                    return;
                }

                int offset = 0;
                while (offset < text.Length)
                {
                    int length = Math.Min(MaxOutputChunkChars, text.Length - offset);
                    // Do not split a UTF-16 surrogate pair across chunks.
                    if (length > 0
                        && offset + length < text.Length
                        && char.IsHighSurrogate(text[offset + length - 1]))
                    {
                        length--;
                    }
                    if (length <= 0)
                    {
                        break;
                    }

                    string chunk = text.Substring(offset, length);
                    string encoded = Convert.ToBase64String(Encoding.UTF8.GetBytes(chunk));
                    string line = "{\"v\":1,\"event\":\"O\",\"id\":"
                        + JsonSerializer.Serialize(commandId)
                        + ",\"data\":\""
                        + encoded
                        + "\"}";
                    lock (_outputDrainLock)
                    {
                        _pendingOutput++;
                    }
                    if (!TryEnqueue(line))
                    {
                        // Queue full/closed: drop this and the remaining
                        // chunks, roll the drain counter back and let D pass.
                        lock (_outputDrainLock)
                        {
                            _pendingOutput--;
                            Monitor.PulseAll(_outputDrainLock);
                        }
                        return;
                    }
                    offset += length;
                }
            }
        }

        /// <summary>
        /// Wait until all queued O frames have been written to the event pipe.
        /// D is queued only after this returns, so service observes O before D.
        /// Timeout is a failure guard, not a normal delay.
        /// </summary>
        internal bool WaitForOutputDrain(int timeoutMilliseconds)
        {
            long deadline = Environment.TickCount64 + timeoutMilliseconds;
            lock (_outputDrainLock)
            {
                while (_pendingOutput > 0)
                {
                    int remaining = (int)(deadline - Environment.TickCount64);
                    if (remaining <= 0)
                    {
                        return false;
                    }
                    Monitor.Wait(_outputDrainLock, remaining);
                }
                return true;
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
                        if (line.Contains("\"event\":\"O\""))
                        {
                            lock (_outputDrainLock)
                            {
                                if (_pendingOutput > 0)
                                {
                                    _pendingOutput--;
                                }
                                Monitor.PulseAll(_outputDrainLock);
                            }
                        }
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
                // A dead event pipe must not make every later command wait the
                // full drain timeout: drop the pending O count and wake any
                // waiter so D is queued immediately.
                lock (_outputDrainLock)
                {
                    _pendingOutput = 0;
                    Monitor.PulseAll(_outputDrainLock);
                }
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
