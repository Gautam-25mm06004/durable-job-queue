import os, sys, tempfile, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from jobq import JobQueue  # noqa
from jobq.supervisor import run_pool  # noqa


class Clock:
    def __init__(self): self.t = 1000.0
    def __call__(self): return self.t


class TestQueue(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(tempfile.mkdtemp(), "q.db")
        self.clock = Clock()
        self.q = JobQueue(self.path, lease_s=10, clock=self.clock)

    def test_lease_is_exclusive_and_expires(self):
        jid, _ = self.q.enqueue({"n": 1})
        a = self.q.lease("A")
        self.assertEqual(a["id"], jid)
        self.assertIsNone(self.q.lease("B"))                  # still leased by A
        self.clock.t += 11                                    # A died; lease expired
        b = self.q.lease("B")
        self.assertEqual((b["id"], b["attempt"]), (jid, 2))
        self.assertFalse(self.q.complete_with(jid, "A", {}))  # A's late ack is rejected
        self.assertTrue(self.q.complete_with(jid, "B", {"ok": 1}))
        self.assertEqual(self.q.counts(), {"done": 1})

    def test_heartbeat_extends_lease(self):
        jid, _ = self.q.enqueue({"n": 1})
        self.q.lease("A")
        self.clock.t += 8; self.assertTrue(self.q.heartbeat(jid, "A"))
        self.clock.t += 8; self.assertIsNone(self.q.lease("B"))

    def test_dedup_key(self):
        a, created_a = self.q.enqueue({"n": 1}, dedup_key="order-42")
        b, created_b = self.q.enqueue({"n": 1}, dedup_key="order-42")
        self.assertEqual((a, created_a, created_b), (b, True, False))

    def test_backoff_and_dead_letter(self):
        jid, _ = self.q.enqueue({"n": 1}, max_attempts=4)
        delays = []
        for attempt in range(1, 5):
            job = self.q.lease("W")
            self.assertEqual(job["attempt"], attempt)
            r = self.q.fail(jid, "W", "boom")
            if attempt < 4:
                self.assertLessEqual(r, min(self.q.cap, self.q.base * 2 ** attempt))
                self.assertIsNone(self.q.lease("W") if r > 0 else None)   # not runnable before its backoff
                self.clock.t += r + 1e-6
                delays.append(r)
            else:
                self.assertEqual(r, "dead")
        self.assertEqual(self.q.counts(), {"dead": 1})

    def test_delayed_job(self):
        self.q.enqueue({"n": 1}, delay=5)
        self.assertIsNone(self.q.lease("W"))
        self.clock.t += 5.1
        self.assertIsNotNone(self.q.lease("W"))

    def test_multiprocess_with_crashes(self):
        q = JobQueue(self.path)
        for i in range(300):
            q.enqueue({"n": i})
        q.close()
        s = run_pool(self.path, 4, crash_prob=0.05, lease_s=0.3, timeout=120)
        self.assertEqual(s["done"], 300)
        self.assertEqual(s["stuck"], 0)
        self.assertEqual(s["effects_rows"], 300)              # exactly one effect per job despite redeliveries
        self.assertEqual(s["wrong_results"], 0)
        self.assertGreater(s["worker_crashes"], 0)


if __name__ == "__main__":
    unittest.main()
