# Decision and blocker text privacy

The service applies the runtime's literal credential-shape redaction policy to
decision questions, resolution rationales, blocker details and blocker resume
conditions, and command reasons. New commands validate the
original input before redaction. The original request hash remains the
idempotency identity, so two different credentials cannot turn different
requests into an identical replay merely because their display text matches.

The sanitized decision text is used in the task mutation, audit payload and
stored command response. Task reads, paginated/list views, replay responses and
workflow context also sanitize legacy decision fields on output. Reading an old
record does not rewrite that record. Source-finalization transactions use their
authoritative task mutation, rather than rebuilding state from a display response.

New command reasons are redacted before audit persistence, including reasons on
commands without decision or blocker fields. Original request hashes still distinguish
changed reasons, and empty original reasons remain invalid. Historical audit bytes
are not rewritten.

Only those free-text fields are transformed. Blocker kinds, decision IDs, actors, versions,
capabilities, source identities and packet hashes retain their existing meaning.
This policy does not erase original operator plan files or historical database
bytes, detect arbitrary secrets, or establish a complete privacy audit for other
free-text fields, artifacts, logs and event payloads.

Regression coverage includes a previously failing initial-decision canary,
durable task/audit/outcome inspection, ordinary replay and conflicting requests
whose redacted text is identical, oversized input refusal before redaction,
legacy task/list/page/replay views without record mutation, and legacy decision
context without authority expansion. Tests use synthetic canaries and no vendor
credentials or model calls.

Blocker regression coverage includes durable task/audit/outcome redaction,
same-request replay, changed-secret conflict despite equal display text,
invalid original blocker-kind refusal, and legacy task/list/page/replay projections
without changing stored historical bytes. This does not sanitize arbitrary
identifiers or establish full privacy coverage for other command fields.

Task objective projections also redact credential-shaped text in task, list,
page, create and command responses, including legacy idempotent replay outcomes.
The original objective remains authoritative for execution, and the original
request hash still distinguishes changed inputs that redact to the same display
text. Reading an old response does not rewrite its historical stored bytes.
This presentation protection does not claim that original plans, prompts or all
other privacy surfaces are free of credentials.
