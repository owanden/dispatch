import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class BenchmarkTests(unittest.TestCase):
    def test_https_workload_writes_verifiable_report_outside_repository(self):
        script = Path(__file__).resolve().parents[1] / "examples" / "benchmark.py"
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "result.json"
            subprocess.run([sys.executable, str(script), "--events", "2", "--retry-every", "0", "--output", str(output)], cwd=directory, capture_output=True, text=True, timeout=30, check=True)
            report = json.loads(output.read_text(encoding="utf-8"))
        self.assertEqual(report["summary"]["delivered"], 2)
        self.assertEqual(report["summary"]["attempts"], 2)
        self.assertEqual(report["summary"]["receiver_status_counts"], {"204": 2})
        self.assertEqual(report["summary"]["receiver_503_to_204"]["count"], 0)
        self.assertEqual({event["event_id"] for event in report["events"]}, {request["event_id"] for request in report["receiver_requests"]})
        self.assertTrue(all(event["enqueue_to_delivered_seconds"] > 0 for event in report["events"]))
        self.assertTrue(all(attempt["worker_seconds"] >= attempt["child_seconds"] > 0 for attempt in report["attempts"]))


if __name__ == "__main__":
    unittest.main()
