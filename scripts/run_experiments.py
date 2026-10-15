"""Durable queue experiments: throughput vs workers, crash recovery with exactly-once effects (vs a naive design),
retries/backoff with poison messages."""
import json, os, sys, tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from jobq import JobQueue  # noqa
from jobq.supervisor import run_pool  # noqa

R = {}


def fresh(n, poison_every=None):
    path = os.path.join(tempfile.mkdtemp(), "q.db")
    q = JobQueue(path)
    for i in range(n):
        q.enqueue({"n": i, "poison": bool(poison_every and i % poison_every == 0)}, max_attempts=5)
    q.close()
    return path

# 1) Throughput vs workers (I/O-bound jobs: 5 ms each)
tp = []
for w in (1, 2, 4, 8, 16):
    s = run_pool(fresh(1000), w, work_s=0.005, lease_s=2.0, seed=w)
    tp.append({"workers": w, "jobs": 1000, "seconds": s["elapsed_s"], "jobs_per_s": round(1000 / s["elapsed_s"], 1), "done": s["done"]})
R["throughput"] = tp

# 2) Crashes: worker dies after doing the work but before acknowledging it
cr = []
for p in (0.0, 0.01, 0.05, 0.10, 0.20):
    s = run_pool(fresh(1000), 4, crash_prob=p, lease_s=0.3, seed=int(p * 100), naive_effects=True)
    cr.append({"crash_prob_per_job": p, "worker_crashes": s["worker_crashes"], "done": s["done"], "stuck": s["stuck"],
               "executions": s["executions"], "jobs_run_twice_or_more": s["jobs_executed_more_than_once"],
               "transactional_effects (rows)": s["effects_rows"], "naive_effects (rows)": s["naive_effect_rows"],
               "naive_duplicates": s["naive_duplicate_effects"], "wrong_results": s["wrong_results"], "seconds": s["elapsed_s"]})
R["crashes"] = cr

# 3) Transient failures + poison messages
s = run_pool(fresh(1000, poison_every=50), 4, fail_prob=0.2, lease_s=1.0, seed=7)
q = JobQueue(fresh(1))
R["retries"] = {"jobs": 1000, "poison_jobs": 20, "transient_failure_prob": 0.2, "done": s["done"], "dead_lettered": s["dead"], "stuck": s["stuck"],
                "executions": s["executions"], "seconds": s["elapsed_s"]}

(ROOT / "results" / "results.json").write_text(json.dumps(R, indent=2))
def tbl(rows):
    cols = list(rows[0]); return "| " + " | ".join(cols) + " |\n|" + "---|" * len(cols) + "\n" + "".join("| " + " | ".join(str(r[c]) for c in cols) + " |\n" for r in rows)
md = "# Results\n\n## Throughput vs worker processes (1,000 jobs x 5 ms)\n\n" + tbl(tp)
md += "\n## Worker crashes after the work, before the ack (1,000 jobs, 4 workers, 0.3 s lease)\n\n" + tbl(cr)
md += "\n## Retries and dead-lettering\n\n" + tbl([R["retries"]])
(ROOT / "results" / "summary.md").write_text(md)
print(md)
