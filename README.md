# Norway Company Registry Change Monitor (BRREG)

[![tests](https://github.com/automa-flow/norway-brreg-company-change-monitor/actions/workflows/tests.yml/badge.svg)](https://github.com/automa-flow/norway-brreg-company-change-monitor/actions/workflows/tests.yml)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Source code of the
[Norway Company Registry Change Monitor](https://apify.com/automa-flow/norway-brreg-company-change-monitor)
Apify Actor. It watches Norwegian organization numbers in BRREG
Enhetsregisteret and reports typed changes: name, address, industry, VAT
registration, bankruptcy, liquidation, group membership, share capital and
deletion, each with before and after values.

It reads BRREG's official open API, including the register's own update
stream. After the first run, a quiet day for a large watchlist costs a few
update-stream requests instead of one lookup per company.

**Just want the data?** Run the hosted Actor from the
[Apify Store](https://apify.com/automa-flow/norway-brreg-company-change-monitor).
The user documentation (input, output, pricing, scheduling) is the Store
listing, kept here as [.actor/README.md](.actor/README.md).

## How a run works

```text
input ─▶ models ─▶ service ─▶ source ─▶ BRREG open API
                     │  ▲
                     ▼  │
             normalize, monitor
                     │
                     ▼
            main: Dataset rows, KVS state, pay-per-event charges
```

1. Capture the newest update id as the run's start cursor.
2. Baseline every organization that has no stored snapshot, by entity lookup.
3. Capture the newest update id again as the run's cutoff.
4. Walk the update stream from the stored cursor to the cutoff for known
   organizations, and from the start cursor to the cutoff for the ones just
   baselined. This closes the race between a baseline and a change published
   during the same run.
5. Re-fetch the current record only for organizations that actually moved.
6. Diff, emit rows, and only then advance the cursor.

| Module | Responsibility |
| --- | --- |
| [models.py](src/norway_brreg_company_change_monitor/models.py) | Input validation per entry and the public vocabulary (statuses, change types) |
| [source.py](src/norway_brreg_company_change_monitor/source.py) | HTTP to the two BRREG endpoints, pagination, retries, strict response-shape checks |
| [normalize.py](src/norway_brreg_company_change_monitor/normalize.py) | Raw record to a comparable snapshot. Pure functions |
| [monitor.py](src/norway_brreg_company_change_monitor/monitor.py) | Snapshot diff to typed changes. Pure functions |
| [service.py](src/norway_brreg_company_change_monitor/service.py) | The run above, with the source client injected so it is testable without Apify |
| [state.py](src/norway_brreg_company_change_monitor/state.py) | Cursor and last successful snapshot per monitor key in the Apify key-value store |
| [lock.py](src/norway_brreg_company_change_monitor/lock.py) | Fail-closed mutex so two runs never read and write the same monitor state at once |
| [billing.py](src/norway_brreg_company_change_monitor/billing.py) | One charge per completed company check, reserved before the source is called |
| [main.py](src/norway_brreg_company_change_monitor/main.py) | The Apify edge: input, Dataset, key-value store, charging, run summary |

`common/` holds the small shared primitives this Actor uses (HTTP client,
retry policy, structured logging, run-health counters). They are copied from
the private repository where several Actors share them.

## Design rules this code keeps

- **No change and could not check are different answers.** `SOURCE_FAILED`,
  `NOT_FOUND` (HTTP 404), `REMOVED` (HTTP 410, purged from open data),
  `PARTIAL` and `INVALID_INPUT` are separate statuses. An HTTP error never
  becomes an empty result.
- **A cursor never moves over an interval that was not fully read.** If any
  page of the update stream fails, the pass is abandoned with the stored
  cursor and snapshots untouched, and the next run retries the same interval.
- **A failed check never overwrites good state.** Changes are always computed
  from the last successful snapshot to a new successful one.
- **One bad entry does not sink the batch.** BRREG rejects a whole
  200-number filter chunk if one value is malformed, so every entry is
  validated on its own before any request is made.
- **Charge only for completed work.** A company check is charged once per run
  when it completes, whether it changed, stayed the same or was confirmed
  absent. Failed checks are not charged.
- **Collect only what the job needs.** Phone numbers, email addresses,
  websites, roles and beneficial owners are published by the register but
  never copied into output.

## Run the tests

The tests use sanitized captures of the public API and never call BRREG.

```sh
python -m venv .venv
. .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.lock
pip install pytest pytest-asyncio ruff mypy
pytest
ruff check . && ruff format --check .
mypy
```

The dependency lock targets CPython 3.12 on Linux, like the Actor image.

## Source data and license

Data comes from the
[BRREG Enhetsregisteret open API](https://data.brreg.no/enhetsregisteret/api/dokumentasjon/no/index.html)
and is published under the
[Norwegian Licence for Open Government Data (NLOD)](https://data.norge.no/nlod/en/2.0).
Attribute Brønnøysundregistrene when you republish it. No authentication,
browser, proxy or CAPTCHA handling is involved.

The code is [MIT licensed](LICENSE). Source comments refer to research probes
under `experiments/`; those notes stay in the private repository this code is
exported from.

## About

Built and maintained by Vadim Bezrukov
([automa-flow on Apify](https://apify.com/automa-flow)). Found a problem with a
run? Open an issue on the
[Actor page](https://apify.com/automa-flow/norway-brreg-company-change-monitor/issues)
with the run ID. Questions about the code are welcome as GitHub issues. If you
need a similar registry or change-monitoring pipeline for your own sources,
you can reach me through the Apify profile.
