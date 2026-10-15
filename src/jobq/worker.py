"""Worker loop used by the experiments. With naive_effects=True the side effect is committed separately before the ack
(a common bug), so crashes produce duplicate effects. `crash_prob` makes the process die abruptly (os._exit) in the middle of a job,
after doing the work but before acknowledging it -- the hardest case for exactly-once effects."""
import os, random, time
from .queue import JobQueue


def worker_main(db_path, name, crash_prob=0.0, fail_prob=0.0, work_s=0.0, lease_s=1.0, idle_exit_s=1.5, seed=None, log_path=None,
                naive_effects=False):
    rng = random.Random(seed)
    q = JobQueue(db_path, lease_s=lease_s)
    idle_since = time.time()
    log = open(log_path, "a") if log_path else None
    while True:
        job = q.lease(name)
        if job is None:
            if time.time() - idle_since > idle_exit_s:
                break
            time.sleep(0.01)
            continue
        idle_since = time.time()
        payload = job["payload"]
        if payload.get("poison"):
            q.fail(job["id"], name, "poison message")
            continue
        if work_s:
            time.sleep(work_s)
        if log:
            log.write(f"{job['id']}\n"); log.flush()        # an "execution" happened (possibly duplicated)
        if naive_effects:      # anti-pattern: apply the side effect in its own transaction before acknowledging
            q.db.execute("INSERT INTO naive_effects (job_id, result) VALUES (?, ?)", (job["id"], str(payload["n"] ** 2)))
        if rng.random() < crash_prob:
            os._exit(1)                                     # die after working, before the ack
        if rng.random() < fail_prob:
            q.fail(job["id"], name, "transient error")
            continue
        q.complete_with(job["id"], name, {"square": payload["n"] ** 2})
    q.close()
