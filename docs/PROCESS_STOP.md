# Confirming controlled process-group stop

## Observed failure

macOS CI40726705160 reported `Stopped`, then a descendant's file grew from one
`tick` to four over350ms. A local diagnostic repeating the same exit/cancel delays
reproduced the failure in32.99s: the group SIGKILL returned success, but the output
grew from one to six ticks after the observation. The normal-exit/cancel distinction
was already implemented, so changing Finished into Stopped is not the missing
check. The exact kernel/fork timing is not established by these observations.

## Required behavior and implementation

A successful group signal is a request, not proof that the group is gone. Timeout,
output-limit and cancellation paths now use the same sequence:

1. Signal the group while the unreaped direct child pins its numeric identity.
2. Make two bounded cleanup attempts before reaping. These are best effort; macOS
   was observed returning EPERM on a retry after the initial successful signal.
3. Reap the direct child, then probe group existence using signal zero. No further
   destructive signal is sent after reaping.
4. Return the original stop reason only after ESRCH proves the group absent.
   Existence beyond250ms, permissions or other probe errors return Fence instead.

The extra cleanup attempts do not establish success. The final absence check is
mandatory. Numeric identity reuse can cause a conservative refusal; the read-only
probe cannot kill an unrelated process. Zombies can also delay or prevent an
absence result, which remains uncertainty rather than a fabricated stop.

The wrapper already refuses to turn Fence into a Stopped frame or terminal receipt.
Normal process exit remains Finished and does not imply descendant fencing. This
is a controlled process-group boundary, not proof against arbitrary descendants
that escape the group; native write admission still depends on the separately
accepted restricted capability profile. Future cancellation/unwind cleanup alone
is not a verified stop observation.

## Evidence scope

A deterministic live-group control proves that the absence check refuses while
the process remains alive, does not kill it, and accepts after explicit cleanup.
Existing runner stop assertions are unchanged. Original failures and diagnostic
candidate failures are retained under private
`reviews/agent-conductor-runtime-20261028/stop-race-diagnostic/`.


The fixed repeat diagnostic passed all120iterations in52.34s (71Stopped,
49Finished), retaining the original no-later-write assertion. This is bounded
stress evidence, not a proof of all OS schedules. The188-test full workspace and
strict Clippy/format/build passed with the original runner test restored. No
original assertion or production timeout was relaxed. Early candidate failures
(group still present; EPERM on a cleanup retry) remain in the diagnostic directory.

The real restricted Claude writer pause/restart/continuation probe also passed.
It paused after answer.txt existed, recorded the observed stop as fenced, restarted
the daemon, resumed into a new attempt/epoch, then completed checks and independent
Codex review in65.97s from continuation. Old bytes remained unchanged and the new
workspace contained the retained copy; stale/terminal continuation was refused.
Private receipt: `writer-pause-v3/receipt.json` under the same review root. Accepted
immutable adapter pins were reused, not refreshed from updated symlinks. Actual
model spend was not measured; the pre-existing configured caps were retained.
