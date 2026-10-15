# Durable Job Queue — Leases, Retries, Dead Letters and Exactly-Once Effects

A background-job queue (like a tiny Sidekiq/Celery backend) built on SQLite in WAL mode and safe for many worker
**processes**:

- **Leases (visibility timeout):** a worker claims a job inside a `BEGIN IMMEDIATE` transaction; if the worker dies, the lease
  expires and another worker gets the job → **at-least-once delivery**. Heartbeats extend long leases.
- **Retries** with exponential backoff and full jitter; after `max_attempts` the job moves to a **dead-letter** state.
- **Idempotent enqueue** with a dedup key.
- **Exactly-once effects:** the handler's result is written in the *same transaction* as the acknowledgement, and only if the
  worker still owns the lease, so a late or duplicate execution can never produce a second effect.

## Results

**Crash recovery** — workers are killed (`os._exit`) after doing the work but *before* acknowledging it; 1,000 jobs, 4 workers,
supervisor restarts dead workers:

| Crash probability per job | Worker crashes | Jobs completed | Jobs executed ≥ 2 times | Effects (transactional ack) | Effects (naive: write, then ack) |
|---|---|---|---|---|---|
| 0% | 0 | 1,000 | 0 | 1,000 | 1,000 |
| 1% | 9 | 1,000 | 9 | **1,000** | 1,009 (9 duplicates) |
| 5% | 53 | 1,000 | 47 | **1,000** | 1,053 (53 duplicates) |
| 10% | 124 | 1,000 | 110 | **1,000** | 1,124 (124 duplicates) |
| 20% | 248 | 1,000 | 198 | **1,000** | 1,248 (248 duplicates) |

Every job finishes, none get stuck, and all results are correct. Re-execution is unavoidable with at-least-once delivery, but
the transactional design keeps **exactly one effect per job**, while the common "apply the side effect, then ack" pattern
duplicates one effect per crash.

**Retries:** with 20% transient failures and 20 poison messages among 1,000 jobs, all 980 good jobs complete and the 20 poison
messages are dead-lettered after 5 attempts.

**Throughput** (I/O-bound 5 ms jobs): 154 jobs/s with 1 worker → 685 jobs/s with 16 (4.5×), limited by SQLite's single writer.

Full tables: [`results/summary.md`](results/summary.md)

## Run
```bash
python -m unittest discover -s tests -v     # lease expiry, late-ack rejection, backoff, DLQ, dedup, multi-process crash test
python scripts/run_experiments.py           # ~35 s
```
```python
from jobq import JobQueue
q = JobQueue("jobs.db")
q.enqueue({"email": "welcome", "user": 42}, dedup_key="welcome-42")
job = q.lease("worker-1")
q.complete_with(job["id"], "worker-1", {"sent": True})
```
