"""A durable job queue on SQLite (WAL mode), safe for many worker processes.

Guarantees: at-least-once delivery (a job whose worker dies is re-delivered after its lease expires), retries with
exponential backoff and full jitter, a dead-letter state after max_attempts, idempotent enqueue (dedup key), and
exactly-once *effects* when handlers record their side effect in the same transaction as the ack (see `complete_with`).
"""
import json, random, sqlite3, time

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  dedup_key TEXT UNIQUE,
  payload TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'ready',          -- ready | leased | done | dead
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 5,
  run_at REAL NOT NULL,                         -- not before this time (backoff)
  lease_owner TEXT, lease_until REAL,
  last_error TEXT, created REAL NOT NULL, finished REAL
);
CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(state, run_at);
CREATE TABLE IF NOT EXISTS effects (job_id INTEGER PRIMARY KEY, result TEXT);   -- side effects, written atomically with ack
CREATE TABLE IF NOT EXISTS naive_effects (job_id INTEGER, result TEXT);        -- used only to demonstrate the anti-pattern
"""


class JobQueue:
    def __init__(self, path, lease_s=2.0, base_backoff=0.05, max_backoff=2.0, clock=time.time):
        self.path, self.lease_s, self.base, self.cap, self.clock = path, lease_s, base_backoff, max_backoff, clock
        self.db = sqlite3.connect(path, timeout=30, isolation_level=None)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.executescript(SCHEMA)

    def enqueue(self, payload, dedup_key=None, max_attempts=5, delay=0.0):
        """Returns (job_id, created). A repeated dedup_key returns the existing job instead of a duplicate."""
        now = self.clock()
        try:
            cur = self.db.execute("INSERT INTO jobs (dedup_key, payload, max_attempts, run_at, created) VALUES (?,?,?,?,?)",
                                  (dedup_key, json.dumps(payload), max_attempts, now + delay, now))
            return cur.lastrowid, True
        except sqlite3.IntegrityError:
            row = self.db.execute("SELECT id FROM jobs WHERE dedup_key = ?", (dedup_key,)).fetchone()
            return row[0], False

    def lease(self, worker):
        """Atomically claim one runnable job: ready and due, or leased with an expired lease (worker died)."""
        now = self.clock()
        self.db.execute("BEGIN IMMEDIATE")          # take the write lock before reading, so two workers can't claim one job
        try:
            row = self.db.execute(
                "SELECT id, payload, attempts FROM jobs WHERE (state = 'ready' AND run_at <= ?) "
                "OR (state = 'leased' AND lease_until < ?) ORDER BY run_at LIMIT 1", (now, now)).fetchone()
            if row is None:
                self.db.execute("COMMIT")
                return None
            self.db.execute("UPDATE jobs SET state='leased', lease_owner=?, lease_until=?, attempts=attempts+1 WHERE id=?",
                            (worker, now + self.lease_s, row[0]))
            self.db.execute("COMMIT")
            return {"id": row[0], "payload": json.loads(row[1]), "attempt": row[2] + 1}
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    def heartbeat(self, job_id, worker):
        cur = self.db.execute("UPDATE jobs SET lease_until=? WHERE id=? AND state='leased' AND lease_owner=?",
                              (self.clock() + self.lease_s, job_id, worker))
        return cur.rowcount == 1

    def complete_with(self, job_id, worker, result):
        """Record the side effect and mark the job done in ONE transaction, only if we still own the lease.
        If a slow worker lost its lease and another worker already finished the job, this is a no-op."""
        self.db.execute("BEGIN IMMEDIATE")
        try:
            owned = self.db.execute("SELECT 1 FROM jobs WHERE id=? AND state='leased' AND lease_owner=?", (job_id, worker)).fetchone()
            if not owned:
                self.db.execute("ROLLBACK")
                return False
            self.db.execute("INSERT OR IGNORE INTO effects (job_id, result) VALUES (?, ?)", (job_id, json.dumps(result)))
            self.db.execute("UPDATE jobs SET state='done', finished=?, lease_owner=NULL WHERE id=?", (self.clock(), job_id))
            self.db.execute("COMMIT")
            return True
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    def fail(self, job_id, worker, error):
        """Retry with exponential backoff and full jitter (sleep ~ U(0, min(cap, base * 2^attempt))), or dead-letter."""
        row = self.db.execute("SELECT attempts, max_attempts FROM jobs WHERE id=? AND lease_owner=?", (job_id, worker)).fetchone()
        if row is None:
            return None
        attempts, max_attempts = row
        if attempts >= max_attempts:
            self.db.execute("UPDATE jobs SET state='dead', last_error=?, lease_owner=NULL, finished=? WHERE id=?", (error, self.clock(), job_id))
            return "dead"
        delay = random.uniform(0, min(self.cap, self.base * 2 ** attempts))
        self.db.execute("UPDATE jobs SET state='ready', run_at=?, last_error=?, lease_owner=NULL WHERE id=?",
                        (self.clock() + delay, error, job_id))
        return delay

    def counts(self):
        return dict(self.db.execute("SELECT state, COUNT(*) FROM jobs GROUP BY state").fetchall())

    def close(self):
        self.db.close()
