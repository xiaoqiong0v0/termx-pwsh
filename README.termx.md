# termx-pwsh

This repository is the termx-maintained fork of
[PowerShell](https://github.com/PowerShell/PowerShell). It adds the shell
lifecycle event channel used by the termx terminal runtime.

## Repositories and branches

- Upstream: https://github.com/PowerShell/PowerShell
- termx fork: https://github.com/xiaoqiong0v0/termx-pwsh
- termx branch: `termx/events`

## termx changes

- `ConsoleHostShellEvents.cs` emits `A/B/C/D/X` command lifecycle events over
  the termx out-of-band channel.
- Events are sent over the host-provided named pipe; nothing is written to the
  shell's stdout/stderr and no OSC marker is injected.
- `X` aborts a pending command record when input is never executed.

## Build and test

Build this fork the same way as upstream PowerShell. The termx changes are
limited to the console-host event bridge; no new runtime dependency is added.

## Syncing with upstream

Keep `upstream` pointed at `PowerShell/PowerShell`, rebase `termx/events` onto
the desired upstream release branch, and resolve any event-bridge changes in
`ConsoleHostShellEvents.cs`.

## Parent project

The parent `opencode-termx-cli` repository includes this repository as the
`terminals/termx-pwsh` submodule.
