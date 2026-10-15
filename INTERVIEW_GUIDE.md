# Interview guide — Durable job queue

## 30-second version
"I built a job queue on SQLite that's safe for many worker processes: jobs are leased with a timeout so if a worker dies the job
comes back, failures retry with exponential backoff and jitter, and poison jobs go to a dead-letter state. I killed workers
in the middle of jobs at up to 20% probability — every job still completed, and because the result is written in the same
transaction as the ack, there was exactly one effect per job. A naive 'do it, then ack' version created one duplicate per crash."

## Must-know
- **At-least-once vs exactly-once**: delivery can't be exactly-once when consumers crash; make *effects* idempotent instead.
- **Lease / visibility timeout**: the job is invisible to others while leased; expiry means "worker presumed dead".
- **BEGIN IMMEDIATE**: takes the write lock up front so two workers can't select the same job.
- **Fencing by owner**: the ack checks `lease_owner` — a slow worker that lost its lease can't complete the job.
- **Backoff with full jitter**: random delay in [0, base·2^n] spreads retries and avoids thundering herds.
- **Dead-letter queue**: stop retrying jobs that will never succeed; keep them for inspection.

## Likely questions
1. *How long should the lease be?* Longer than the typical job (or use heartbeats); too short → needless duplicates, too long → slow recovery.
2. *What if the side effect is an external API call?* Pass the job id as an idempotency key to the API, or use an outbox table.
3. *Why only 4.5× with 16 workers?* SQLite has one writer; claims and acks serialise. Postgres with `SELECT … FOR UPDATE SKIP LOCKED` scales further.
4. *How do you test crash safety?* Kill processes with `os._exit` at the worst moment and check invariants: no stuck jobs, one effect per job.
