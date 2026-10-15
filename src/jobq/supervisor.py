"""Run N worker processes, restart any that crash, until the queue drains. Returns run statistics."""
import multiprocessing as mp, os, time
from collections import Counter
from .queue import JobQueue
from .worker import worker_main


def run_pool(db_path, n_workers, crash_prob=0.0, fail_prob=0.0, work_s=0.0, lease_s=0.5, timeout=300, seed=0, naive_effects=False):
    log = db_path + ".exec.log"
    if os.path.exists(log):
        os.remove(log)
    ctx = mp.get_context("fork")
    procs, restarts, k = {}, 0, 0
    t0 = time.time()
    def spawn(i):
        nonlocal k
        k += 1
        p = ctx.Process(target=worker_main, args=(db_path, f"w{i}-{k}", crash_prob, fail_prob, work_s, lease_s, 1.0, seed * 1000 + k, log, naive_effects))
        p.start(); return p
    for i in range(n_workers):
        procs[i] = spawn(i)
    q = JobQueue(db_path)
    while time.time() - t0 < timeout:
        time.sleep(0.05)
        c = q.counts()
        pending = c.get("ready", 0) + c.get("leased", 0)
        for i, p in list(procs.items()):
            if not p.is_alive():
                p.join()
                if p.exitcode != 0 and pending:
                    restarts += 1
                    procs[i] = spawn(i)
                elif pending:
                    procs[i] = spawn(i)
                else:
                    procs.pop(i)
        if not procs and not pending:
            break
    for p in procs.values():
        p.join(timeout=5)
    elapsed = time.time() - t0
    execs = Counter()
    if os.path.exists(log):
        with open(log) as fh:
            execs = Counter(int(l) for l in fh)
    c = q.counts()
    effects = q.db.execute("SELECT COUNT(*), COUNT(DISTINCT job_id) FROM effects").fetchone()
    wrong = q.db.execute("SELECT COUNT(*) FROM effects e JOIN jobs j ON j.id = e.job_id "
                         "WHERE CAST(json_extract(e.result, '$.square') AS INTEGER) != CAST(json_extract(j.payload, '$.n') AS INTEGER) * CAST(json_extract(j.payload, '$.n') AS INTEGER)").fetchone()[0]
    naive = q.db.execute("SELECT COUNT(*), COUNT(DISTINCT job_id) FROM naive_effects").fetchone()
    q.close()
    return {"naive_effect_rows": naive[0], "naive_duplicate_effects": naive[0] - naive[1],"elapsed_s": round(elapsed, 2), "done": c.get("done", 0), "dead": c.get("dead", 0), "stuck": c.get("ready", 0) + c.get("leased", 0),
            "executions": sum(execs.values()), "jobs_executed_more_than_once": sum(1 for v in execs.values() if v > 1),
            "effects_rows": effects[0], "effects_distinct_jobs": effects[1], "wrong_results": wrong, "worker_crashes": restarts}
