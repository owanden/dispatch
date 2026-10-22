# Installable wheel

The `owanden-dispatch` distribution installs the `dispatch` package and `dispatch-api` / `dispatch-worker` commands. It requires Python 3.12+ and has no third-party runtime dependencies. The wheel contains runtime code and distribution metadata; demos, benchmarks, private files, and databases are not bundled. A downloadable wheel is published on GitHub Releases; no package-index publication or hosted deployment is configured.

## Download without a checkout

Open [Dispatch v0.1.0](https://github.com/owanden/dispatch/releases/tag/v0.1.0) and download `requirements.txt` from its assets. Create and activate a virtual environment as shown below, then install:

```sh
python -m pip install --no-deps --require-hashes -r requirements.txt
```

The file pins the exact wheel URL and SHA256, so pip rejects bytes that do not match. The installation needs GitHub network access, but no GitHub sign-in, source checkout, build tools or package-index lookup. The release also includes the wheel for offline installation, `SHA256SUMS`, and `release.json` identifying the source commit, build environment and actual package verification. Download the wheel in advance to use the offline command below, adjusting its path to the downloaded file.

Checksums detect mismatched downloads; they are not a separate publisher signature. Local verification uses the exact uploaded wheel in a fresh environment. Windows/Linux CI separately builds and checks the same source commit; its wheel files may differ in archive metadata. This is not a byte-identical build claim. The release excludes demos, which remain runnable from a source checkout.

## Build and verify

From the repository root:

```sh
python -m pip wheel --no-deps --wheel-dir dist .
python tools/smoke_package.py dist/owanden_dispatch-0.1.0-py3-none-any.whl
```

Building uses isolated pinned setuptools/wheel build dependencies from `pyproject.toml`; the first build needs access to those packages. This is a repeatable build procedure, not a claim of byte-identical builds. Generated distributions and build metadata are ignored by Git.

The smoke command creates a fresh temporary virtual environment, installs only the wheel with package-index access disabled, and runs outside the source tree using isolated Python imports. It checks both console commands, starts the installed API, enqueues an event, terminates/restarts the API against the same database, and confirms persisted event identity/idempotency. It then runs the installed worker with a deliberately different allowed origin, verifies that its delivery child records a durable policy rejection, and cleans up. It sends no external requests. This package smoke verifies process/import/storage wiring; the test suite separately verifies real local HTTPS success and failure paths.

## Run an installed copy

Create and activate a virtual environment:

```sh
python -m venv .venv
```

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

Linux/macOS:

```sh
. .venv/bin/activate
```

Then install the built wheel and run the API:

```sh
python -m pip install --no-index --no-deps dist/owanden_dispatch-0.1.0-py3-none-any.whl
dispatch-api --db events.db --port 8080 --allow-origin https://example.com
```

In a second terminal with the same environment activated, run the worker against the same database path:

```sh
dispatch-worker --db events.db --allow-origin https://example.com --continuous
```

Use an explicit absolute database path if the terminals have different working directories. Keep the database on persistent local storage when restarting processes. The commands also remain available as `python -m dispatch.api` and `python -m dispatch.worker`.

The API binds to loopback and has no authentication. Set permitted origins explicitly and configure optional signing through the worker environment as described in [signed deliveries](signatures.md). See [operations](operations.md) for retry, deadline, lease, and duplicate-delivery limits. Installing a wheel does not provide a process supervisor, backup policy, public ingress, authentication, or production deployment.

## CI artifacts

The [Tests workflow](../.github/workflows/test.yml) builds the wheel and runs the isolated smoke on Windows and Linux with Python 3.12. Successful packaging jobs retain the wheel as a GitHub Actions artifact named `dispatch-wheel-Windows` or `dispatch-wheel-Linux`. Check the workflow's exact commit and successful result before downloading an artifact. The normal Linux source test job remains separate. These jobs retain per-run artifacts; they do not publish to PyPI, create releases, start containers, or provision infrastructure. The separately published versioned release above retains its own exact wheel and verification record.
