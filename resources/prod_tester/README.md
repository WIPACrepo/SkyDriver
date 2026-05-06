# `resources/prod_tester/`

> Manual testing tools for validating a live SkyDriver instance.
> Neither script is part of CI — they require a human, a real cluster, and
> real Keycloak credentials.

Both scripts accept `--skydriver dev|prod` to target the dev or prod instance respectively.

<br>

## Scripts

### 🏋️ `prod_tester.py` — Full integration test suite

Runs (or re-runs) the `skymap_scanner` test suite against a live SkyDriver
instance and compares results against upstream expected outputs.

**Use this when:**

- Validating a new SkyDriver release before promoting it to prod
- Verifying a significant backend change (new cluster, new `skyscan` image, etc.)
- Something is suspected to be broken end-to-end and you need a definitive pass/fail

**What it does:**

1. Downloads the `skymap_scanner` test events + expected results from GitHub
2. Launches real scans on a compute cluster via SkyDriver
3. Monitors each scan until completion
4. Diffs actual vs. expected results using `compare_scan_results.py`
5. Reports pass/fail per test and exits non-zero on any failure

**Key flags:**

| Flag                         | Notes                                                     |
|------------------------------|-----------------------------------------------------------|
| `--skydriver dev\|prod`      | Target instance                                           |
| `--cluster CLUSTER`          | e.g. `osg`                                                |
| `--n-workers N`              | Workers per scan                                          |
| `--skyscan-docker-tag TAG`   | Defaults to `latest`                                      |
| `--repull-tests true\|false` | Force re-download of upstream test files                  |
| `--one`                      | Run only a single (faster) test instead of the full suite |
| `--rescan [PATH]`            | Re-run scans from a previous sandbox dir or `.tar`        |
| `--compare-only`             | Skip launching; just diff results from the last sandbox   |
| `--dry-run`                  | Build the test plan but don't send anything to SkyDriver  |

> 📦 **Sandbox:** `prod_tester.py` creates a `test-suite-sandbox/` directory alongside itself.
> Each run rotates the previous sandbox to a `.tar` archive. Cached upstream files
> (events, expected results, compare script) are preserved across rotations.

**Example:**

```bash
python prod_tester.py \
  --skydriver dev \
  --cluster osg \
  --n-workers 1000 \
  --repull-tests false
```

<br>

### 🔍 `random_query.py` — REST API smoke test

Walks the SkyDriver REST API broadly and confirms everything that should be
accessible is accessible. No scan launches, no result comparison — purely a
read-only connectivity and sanity check.

**Use this when:**

- Quickly checking that a SkyDriver instance is healthy and all endpoints respond
- Verifying a REST API change (new routes, changed projections, auth updates)
  didn't break existing access patterns
- Doing a routine spot-check without spinning up real compute

**What it does:**

1. Fetches all scan IDs via `POST /scans/find`
2. Bulk-fetches full manifests in paginated chunks
3. Categorizes scans by schema version (`>= v1.2` vs. older)
4. Queries `GET /scans/backlog`
5. Queries each scan individually across four endpoints:
   `/scan/{id}`, `/scan/{id}/manifest`, `/scan/{id}/i3-event`, `/scan/{id}/result`

**Key flags:**

| Flag                    | Notes                                                            |
|-------------------------|------------------------------------------------------------------|
| `--skydriver dev\|prod` | Target instance                                                  |
| `--sample N`            | Fraction of scans to query (e.g. `0.1` = 10%); omit to query all |

**Example:**

```bash
# Full run against prod
python random_query.py --skydriver prod

# Quick spot-check: 5% sample
python random_query.py --skydriver dev --sample 0.05
```

<br>

## 🛠️ Setup

```bash
pip install -r requirements.txt
```

Dependencies for both scripts are combined in the single `requirements.txt` in this directory.

<br>

## 🔐 Auth

Both scripts use `SavedDeviceGrantAuth` (device grant flow via Keycloak).
On first run, a QR code is presented in the terminal for validation.
The refresh token is cached at `~/device-refresh-token-skydriver[-dev]`
for subsequent runs.
