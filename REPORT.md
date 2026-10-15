# At-Least-Once Delivery with Exactly-Once Effects: A SQLite Job Queue Under Process Crashes

**Gautam Jain** — IIT Bhubaneswar

## 1. Problem
A job queue cannot both guarantee that every job runs and that it runs only once when workers can die at any moment: if a
worker crashes after doing the work but before acknowledging it, the system cannot know whether the work happened. Practical
systems therefore deliver at least once and make effects idempotent.

## 2. Design
- **Storage**: one SQLite table in WAL mode (concurrent readers, one writer) with an index on (state, run_at).
- **Claim**: `BEGIN IMMEDIATE` takes the write lock, selects the oldest job that is ready and due or whose lease has expired,
  and marks it leased with owner and expiry, incrementing `attempts`.
- **Acknowledge**: `complete_with` re-checks inside a write transaction that the caller still owns the lease, inserts the
  effect keyed by job id, and marks the job done — atomically. A worker whose lease expired and was taken over cannot
  acknowledge, and a second effect for the same job is impossible.
- **Failure**: `attempts < max_attempts` → back to ready with `run_at = now + U(0, min(cap, base·2^attempts))` (exponential
  backoff with full jitter); otherwise dead.
- **Dedup**: a UNIQUE dedup key makes producer retries safe.

## 3. Evaluation
A supervisor runs N worker processes and restarts any that exit abnormally. Workers log each execution to a file before
possibly crashing with `os._exit(1)` (no cleanup, no ack). A "naive" variant also commits its side effect in a separate
transaction before the ack, as many hand-written workers do. Lease 0.3 s.

## 4. Results
See README. Across crash probabilities up to 20% per job, all 1,000 jobs completed with zero stuck jobs and zero wrong results;
executions exceeded jobs by exactly the number of crashes, and the transactional effects table always held exactly 1,000 rows
while the naive table held one duplicate per crash. Throughput scaled sub-linearly (4.5× at 16 workers) because every claim
and ack serialises on SQLite's writer lock.

## 5. Limitations
Single machine (SQLite file); polling rather than notifications; effects stored in the same database (for external side
effects such as payments you need an idempotency key at the external service, or an outbox pattern).

## References
1. M. Kleppmann. *Designing Data-Intensive Applications*, ch. 11 (stream processing, exactly-once, idempotence). O'Reilly, 2017.
2. AWS Architecture Blog. *Exponential Backoff and Jitter*, 2015.
3. SQLite documentation: Write-Ahead Logging; BEGIN IMMEDIATE transactions.
