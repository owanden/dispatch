import subprocess
import sys
import unittest
from pathlib import Path


class DemoTests(unittest.TestCase):
    def test_local_https_demo_completes_from_outside_repository(self):
        script = Path(__file__).resolve().parents[1] / "examples" / "demo.py"
        result = subprocess.run([sys.executable, str(script)], cwd=script.parent, capture_output=True, text=True, timeout=30, check=True)
        self.assertIn("First delivery: HTTP 503", result.stdout)
        self.assertIn("Second delivery: HTTP 204; delivered", result.stdout)
        self.assertIn("Restart: event, attempt history, and retry schedule retained", result.stdout)
        self.assertIn("PASS: local HTTPS demo", result.stdout)


if __name__ == "__main__":
    unittest.main()
