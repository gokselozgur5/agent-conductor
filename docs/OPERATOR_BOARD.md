# Local operator board

The optional board displays authoritative task state, worker/verifier identities,
required checks and source-bound acceptance evidence. It is a read-only view of
the existing service, not a second lifecycle engine. It does not start model
calls or mutate tasks.

Set `CONDUCTOR_OPERATOR_TOKEN` in the launching environment without putting its
value in command arguments or shell history. For an interactive shell:

```sh
read -r -s CONDUCTOR_OPERATOR_TOKEN
export CONDUCTOR_OPERATOR_TOKEN
./target/debug/conductor-daemon \
  --database /absolute/private/state.sqlite3 \
  --socket /absolute/private/conductor.sock \
  --lock /absolute/private/conductor.lock \
  --http 127.0.0.1:8787
```

Enter a 32–256 printable-character credential at the silent prompt. Open the
non-secret address printed by the daemon and enter that same credential in the
board's password field. Clear the shell environment when finished. The token is
not included in HTML, URLs, logs, localStorage or sessionStorage. The board state
retains its digest; the browser keeps the credential in tab memory and clears the
field after connecting. Disconnect clears displayed task data and the tab's
credential reference. Native worker environments do not inherit this token.

The listener refuses non-loopback binding. Use the exact numeric address printed
by the daemon: Host and any supplied Origin must match it. API requests require
a bearer credential; static login assets contain no task information. Responses
use restrictive CSP and no-sniff headers; task data replies are not cached.
Service queries have a three-second deadline and one-megabyte response limit.
The daemon bounds board shutdown to five seconds.

The board refreshes every three seconds and labels connection failures as stale
data. Required checks are shown as pending, passed or failed. Candidate model
text never turns a task green; only the service's terminal state is displayed.
Repository/task strings are rendered using text nodes, not HTML injection.

`--http` can accompany `--run-plan`. In that mode the board stops when the batch
invocation exits. Run the ordinary daemon without `--run-plan` for persistent
inspection of existing tasks. Existing CLI mutation commands remain available
through the Unix socket; this board currently adds no task mutation controls.

## Validation and scope

Loopback integration tests verify unauthenticated refusal, hostile Host/Origin
refusal, fresh service queries, token absence from responses and non-loopback/
invalid-token binding refusal. An isolated headless Chrome run rendered two
actual completed synthetic workflows and a third HTML-injection fixture. Worker,
reviewer, passed checks and acceptance were visible; the injected string remained
text and did not execute. Browser storage and URL fragments stayed empty, and
disconnect removed the cards. A second Chrome run also verified wrong-token
refusal, automatic display of a newly created task and a stale-state label after
the daemon stopped. Private screenshot/receipt evidence lives under
`reviews/agent-conductor-runtime-20261028/operator-board-v1` and `operator-board-v2`
in the workspace.

This does not claim remote multi-user authentication, exhaustive HTTP load
hardening, task controls, pagination or mobile-browser acceptance. Those remain
separate scope from this local read-only operator view.
