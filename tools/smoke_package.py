import argparse
import http.client
import json
import os
import socket
import subprocess
import tempfile
import time
import venv
from pathlib import Path


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def request(port, method, path, body=None):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
    try:
        connection.request(method, path, json.dumps(body) if body is not None else None, {"Content-Type": "application/json", "Idempotency-Key": "package-smoke"})
        response = connection.getresponse()
        return response.status, json.loads(response.read())
    finally:
        connection.close()


def stop(process):
    process.terminate()
    try:
        process.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate(timeout=5)


def run(wheel, directory):
    environment = directory / "env"
    venv.EnvBuilder(with_pip=True).create(environment)
    scripts = environment / ("Scripts" if os.name == "nt" else "bin")
    python = scripts / ("python.exe" if os.name == "nt" else "python")
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env.pop("DISPATCH_SIGNING_KEY", None)
    subprocess.run([str(python), "-I", "-m", "pip", "--isolated", "install", "--no-index", "--no-deps", str(wheel)], cwd=directory, env=env, check=True, capture_output=True, text=True, timeout=60)
    result = subprocess.run([str(python), "-I", "-c", "import dispatch; print(dispatch.__file__)"], cwd=directory, env=env, check=True, capture_output=True, text=True, timeout=10)
    require(Path(result.stdout.strip()).is_relative_to(environment), "import escaped installed environment")
    for name in ("dispatch-api", "dispatch-worker"):
        executable = scripts / (name + (".exe" if os.name == "nt" else ""))
        subprocess.run([str(executable), "--help"], cwd=directory, env=env, check=True, capture_output=True, text=True, timeout=10)
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    db = directory / "events.db"
    command = [str(python), "-I", "-m", "dispatch.api", "--db", str(db), "--port", str(port), "--allow-origin", "https://example.com"]
    body = {"destination": "https://example.com/webhook", "payload": {"package": "smoke"}}
    event = None
    for restart in range(2):
        server = subprocess.Popen(command, cwd=directory, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic() + 10
            while True:
                require(server.poll() is None, "installed API exited during startup")
                try:
                    status, _ = request(port, "GET", "/metrics")
                    require(status == 200, "installed API metrics failed")
                    break
                except OSError:
                    require(time.monotonic() < deadline, "installed API startup timed out")
                    time.sleep(0.05)
            status, current = request(port, "POST", "/events", body)
            require(status == 202 and current["status"] == "pending", "installed enqueue failed")
            if restart:
                require(current["id"] == event["id"], "API restart lost event/idempotency")
            event = current
        finally:
            stop(server)
    worker = subprocess.run([str(python), "-I", "-m", "dispatch.worker", "--db", str(db), "--allow-origin", "https://other.example"], cwd=directory, env=env, check=True, capture_output=True, text=True, timeout=30)
    require(json.loads(worker.stdout)["processed"] is True, "installed worker did not process event")
    inspect = subprocess.run([str(python), "-I", "-c", "import json,sys; from dispatch.store import EventStore; s=EventStore(sys.argv[1]); e=s.get(sys.argv[2]); a=s.attempts(e.id); print(json.dumps([e.status,len(a),a[0].error]))", str(db), event["id"]], cwd=directory, env=env, check=True, capture_output=True, text=True, timeout=10)
    require(json.loads(inspect.stdout) == ["failed", 1, "delivery_rejected"], "installed delivery child/outcome persistence failed")
    print("PASS: isolated wheel install, console entry points, API restart/idempotency, installed delivery child and durable policy rejection; no external requests")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("wheel", type=Path)
    args = parser.parse_args()
    wheel = args.wheel.resolve(strict=True)
    with tempfile.TemporaryDirectory(prefix="dispatch-package-") as directory:
        run(wheel, Path(directory))


if __name__ == "__main__":
    main()
