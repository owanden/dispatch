import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class CrashDemoTests(unittest.TestCase):
    def test_terminated_worker_retains_claim_and_receiver_deduplicates_redelivery(self):
        script = Path(__file__).resolve().parents[1] / "examples" / "crash_demo.py"
        with tempfile.TemporaryDirectory(prefix="dispatch-crash-check-") as directory:
            output = Path(directory) / "report.json"
            result = subprocess.run([sys.executable, str(script), "--output", str(output)],
                                    cwd=directory, capture_output=True, text=True, timeout=140, check=True)
            report = json.loads(output.read_text(encoding="utf-8"))
        self.assertNotEqual(report["killed_worker_returncode"], 0)
        self.assertEqual(report["state"], "delivered")
        self.assertEqual([item["error"] for item in report["attempts"]], ["lease_expired", None])
        self.assertEqual([item["status_code"] for item in report["attempts"]], [None, 204])
        self.assertTrue(all(item["finished_at"] is not None for item in report["attempts"]))
        self.assertEqual(len({item["id"] for item in report["attempts"]}), 2)
        self.assertEqual([item["event_id"] for item in report["receiver_requests"]], [report["event_id"]] * 2)
        self.assertEqual(report["receiver_effect_rows"], 1)
        self.assertGreaterEqual(report["recovery"]["observed_at"], report["lease_expires_at"])
        self.assertGreaterEqual(report["receiver_requests"][1]["received_at"], report["recovery"]["retry_at"])
        self.assertGreaterEqual(report["elapsed_seconds"], report["lease_seconds"] + report["retry_delay_seconds"])
        self.assertIn("Reopen: event delivering, unfinished attempt and lease retained", result.stdout)
        self.assertIn("PASS: real worker termination and recovery without clock advancement", result.stdout)


if __name__ == "__main__":
    unittest.main()
