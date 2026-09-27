# Norway Company Registry Change Monitor - BRREG

Watch Norwegian organization numbers and get **typed registry changes**: name,
address, industry, VAT registration, bankruptcy, liquidation, group membership,
share capital, registration data and deletion. Before-and-after values on every
normalized change, no re-export and no diffing on your side. Source changes outside
the normalized fields are reported as `OTHER_CHANGED` with their available evidence.

It reads the [official BRREG Enhetsregisteret open API](https://data.brreg.no/enhetsregisteret/api/dokumentasjon/no/index.html),
including BRREG's own **update stream**. That is the point: after the first run,
a watchlist of 5,000 companies costs **25 requests** on a quiet day instead of
5,000 lookups.

## What it detects

| Change type | Fires when |
| --- | --- |
| `NAME_CHANGED` | The registered name changed |
| `LEGAL_FORM_CHANGED` | The organization form code changed (AS, ASA, ENK, ...) |
| `ADDRESS_CHANGED` | The business or postal address city or municipality changed |
| `INDUSTRY_CHANGED` | Any of the three NACE industry codes changed |
| `EMPLOYEE_COUNT_CHANGED` | The registered number of employees changed |
| `VAT_STATUS_CHANGED` | Entry in or exit from the VAT register (Merverdiavgiftsregisteret) |
| `BANKRUPTCY_STATUS_CHANGED` | The `konkurs` flag changed |
| `LIQUIDATION_STATUS_CHANGED` | Voluntary liquidation or forced dissolution changed |
| `GROUP_STATUS_CHANGED` | The company joined or left a group |
| `SHARE_CAPITAL_CHANGED` | Registered share capital amount or currency changed |
| `REGISTRATION_DATA_CHANGED` | The Enhetsregisteret registration date changed, or a company appeared |
| `DELETION_STATUS_CHANGED` | The company was deleted, or removed from open data |
| `OTHER_CHANGED` | A fresh BRREG operation is outside the exact normalized field paths, including other register memberships or whole-object replacements. The patch is evidence; no before/after values are invented |

Each row also carries `source_change_type`, which is BRREG's own event:
`ENTITY_ADDED`, `ENTITY_CHANGED`, `ENTITY_DELETED`, `REMOVED_FROM_OPEN_DATA` or
`UNKNOWN_SOURCE_CHANGE`.

## Where it fits

- **KYB and supplier-risk teams** watching counterparties for bankruptcy,
  liquidation and legal-form changes.
- **Accounting and accounts-receivable operators** watching VAT registration and
  deletion before invoicing.
- **CRM and master-data owners** keeping Norwegian company records current.
- **Agencies, integrators and agents** that need a machine-readable change feed
  rather than another full registry export.

## 30-second quickstart

1. Put your organization numbers in **Watchlist**. Nine digits each. `NO 923 609 016 MVA`,
   `923-609-016` and `923609016` are all the same company.
2. Give the list a **Monitor key**, such as `suppliers-no`. Keep that key: it owns
   the stored baseline and the update cursor.
3. Leave **What to return** on the prefilled `snapshotAndChanges` for a visible
   record for each company. For scheduled alerts, choose `changesOnly`.

```json
{
  "organizationNumbers": ["923609016", "984851006", "810034882"],
  "monitorKey": "suppliers-no",
  "mode": "snapshotAndChanges",
  "baselineMode": "emitSnapshot",
  "includeSourcePatch": true
}
```

The first run returns a `BASELINE` for every company it resolves. In
`changesOnly`, every run after that returns only what moved. Set **First run under a monitor key** to `storeOnly`
if you want the baseline stored silently, so your first alert is a real change.

Set **What to return** to `snapshotAndChanges` if you also want the current record
for unchanged companies. Those rows are republished from stored state, so they
cost you no extra source requests.

An empty Dataset in `changesOnly` is a successful run with nothing to report.
`RUN_SUMMARY` tells you which it was; never read an empty Dataset as companies
disappearing.

## A change row

```json
{
  "record_type": "CHANGE",
  "status": "SUCCESS",
  "organization_number": "923609016",
  "company_name": "EQUINOR NORGE ASA",
  "event_id": 25175412,
  "event_published_at": "2026-09-08T05:00:11.402Z",
  "source_change_type": "ENTITY_CHANGED",
  "change_types": ["NAME_CHANGED", "VAT_STATUS_CHANGED", "LIQUIDATION_STATUS_CHANGED", "OTHER_CHANGED"],
  "changes": {
    "name": {"previous": "EQUINOR ASA", "current": "EQUINOR NORGE ASA"},
    "vat_registered": {"previous": true, "current": false},
    "under_liquidation": {"previous": false, "current": true}
  },
  "source_patch": [
    {"op": "replace", "path": "/navn", "value": "EQUINOR NORGE ASA"},
    {"op": "replace", "path": "/underAvvikling", "value": true},
    {"op": "replace", "path": "/registrertIMvaregisteret", "value": false},
    {"op": "replace", "path": "/epostadresse", "value_redacted": true}
  ],
  "current": {"organization_number": "923609016", "bankrupt": false, "employees": 21239, "...": "..."},
  "source_id": "brreg-update:25175412",
  "source_url": "https://data.brreg.no/enhetsregisteret/api/enheter/923609016",
  "scraped_at": "2026-09-08T06:05:00Z",
  "fingerprint": "..."
}
```

`changes` contains normalized before/after values. `OTHER_CHANGED` can appear
with an empty `changes` object: the source reported an operation on data this
Actor does not compare field by field. `source_patch` is BRREG's own evidence,
with private values minimized. At most 200 operations are attached;
`source_patch_truncated: true` marks a longer patch. Classification and
disagreement checks use every operation even when the attached patch is shortened.
See
`examples/sample_output.json` for the full row.

## Scheduling and webhooks

Run it on a schedule with the same `monitorKey`. Hourly, daily and weekly all
work; the cursor picks up exactly where the last successful run stopped, so a
missed run costs nothing but latency.

Add an Apify **Schedule** (Apify Console -> Schedules) with cron `0 6 * * *` for a
daily 06:00 UTC check, and a **Webhook** on `ACTOR.RUN.SUCCEEDED` pointing at your
endpoint:

```json
{
  "eventTypes": ["ACTOR.RUN.SUCCEEDED"],
  "requestUrl": "https://example.com/hooks/brreg",
  "payloadTemplate": "{\"runId\": {{resource.id}}, \"datasetId\": {{resource.defaultDatasetId}}}"
}
```

Read the change-focused columns, then keep only rows whose `record_type` is
`CHANGE` in your integration. Dataset views select columns, not rows:

```text
GET https://api.apify.com/v2/datasets/<datasetId>/items?view=changes&clean=true
```

This Actor does not create schedules, send email or configure webhooks for you.

## API and MCP

Start a run and read its Dataset:

```bash
curl -X POST "https://api.apify.com/v2/acts/automa-flow~norway-brreg-company-change-monitor/runs?maxTotalChargeUsd=0.01" \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"organizationNumbers":["923609016","984851006"],"monitorKey":"suppliers-no"}'
```

Synchronous run with items returned directly:

```bash
curl -X POST "https://api.apify.com/v2/acts/automa-flow~norway-brreg-company-change-monitor/run-sync-get-dataset-items?maxTotalChargeUsd=0.01" \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"organizationNumbers":["923609016"],"monitorKey":"suppliers-no","mode":"snapshotAndChanges"}'
```

The Actor is exposed through the Apify MCP server, so an agent can call it as a
tool. The direct MCP address for authenticated clients is:

```
https://mcp.apify.com?tools=automa-flow/norway-brreg-company-change-monitor
```

Connect with your own Apify account. The input schema above is the whole
contract: a watchlist, a monitor key and three switches. There is no concurrency,
retry, chunk-size or cursor knob for an agent to get wrong.

Output surfaces: `changes`, `current`, `diagnostics` and `monitoring` are
column views of the emitted rows. Filter `record_type` and `status` in your
integration. `RUN_SUMMARY` in the key-value store reports run health and the cursor.
An authenticated direct MCP tool and its input schema were verified before
publication. Public search indexing and accountless agentic payments require
separate post-publication verification.

## Pricing

Pay per event with **platform usage included**. No proxy or API-key charges.

| Event | FREE / BRONZE | SILVER | GOLD / PLATINUM / DIAMOND |
| --- | ---: | ---: | ---: |
| `company-monitored` (one verified company check) | **$0.0005** | $0.00045 | $0.0004 |
| `apify-actor-start` (one run) | **$0.005** | $0.005 | $0.005 |

The fixed start fee applies even if input is invalid, the source fails, the run
finds no changes, or it only recovers previous output. Apify charges it once;
memory is capped at 512 MB. Company-check fees follow the success rules below.

An organization is charged once per run whether it changed, stayed identical, or
is confirmed absent from the register. A number listed twice is charged once.
A quiet `changesOnly` run still performs and charges verified checks; its Dataset
may be empty. `RUN_SUMMARY.charged_targets` reconciles these checks. The summary
counts company events; Apify's run charge breakdown also includes the start fee.

| Watchlist | Checks at FREE / BRONZE | Total per run, including start | 30 daily runs |
| --- | ---: | ---: | ---: |
| 1 company | $0.0005 | $0.0055 | $0.165 |
| 3-company demo | $0.0015 | $0.0065 | $0.195 |
| 200 companies | $0.10 | $0.105 | $3.15 |
| 1,000 companies | $0.50 | $0.505 | $15.15 |
| 5,000 companies | $2.50 | $2.505 | $75.15 |

**Never charged as company checks:** invalid organization numbers, internal
retries, failed or incomplete update intervals, contradictory or unreadable
records, recovery-only delivery, replayed update events, and organizations beyond
the run's charge limit. The fixed start fee still applies.

Set **Maximum charge per run** to at least `$0.005 + companies * tier price`.
The default ceiling is $3; the smallest selectable ceiling is $0.01, which is a
spending limit, not a minimum bill. One run accepts at most 5,000 input entries.
If the limit cannot fund the whole watchlist, organizations beyond it come back
as `SKIPPED`, their snapshots stay intact, and the update cursor is not advanced.

## Failure semantics

These are different states, and this Actor never collapses them:

| State | Meaning |
| --- | --- |
| No change rows | Every watched company was checked and none moved |
| `NOT_FOUND` | BRREG answered `404` with an empty body: no record for this valid organization number. A `404` that carries an error body means BRREG could not route the request, and the company is reported as `SOURCE_FAILED` instead |
| `REMOVED` | BRREG answered `410 Gone`: removed from open data, cached record purged. Later snapshots keep reporting it as `REMOVED` |
| `DELETION_STATUS_CHANGED` with `deleted: true` | BRREG still publishes the company, marked deleted with a `slettedato` |
| `SOURCE_FAILED` | The register could not be verified. No company check is charged and good state is preserved |
| `PARTIAL` | The official patch states a value the fetched record does not show and has not moved to. Both are published even in changesOnly; the snapshot and cursor are preserved for retry |
| `INVALID_INPUT` | The entry is not a nine-digit organization number |

If any part of an update interval cannot be read, the whole pass is abandoned:
no change rows, no company-check charges, and the stored cursor stays where it was. Running
again retries the same interval. A cursor is never advanced over an interval that
was not fully read, because that would lose those changes permanently.

A known company with a malformed or contradictory record also pins the cursor.
Successful companies keep their new snapshots and acknowledged event ids, so
retrying that interval does not emit their already processed events again.
Failed first-time lookups are retried as baselines on the next run.

## Source and freshness

Enhetsregisteret is the Norwegian legal-entity register operated by
Brønnøysundregistrene. This Actor reads only its public open-data endpoints:

- `GET /enhetsregisteret/api/oppdateringer/enheter` for the update stream
- `GET /enhetsregisteret/api/enheter/{orgnr}` for the current record

No authentication, no Maskinporten, no authorized endpoint, no browser, no proxy
and no CAPTCHA handling. Freshness is BRREG's own: the update stream carries the
timestamp of each event and `event_published_at` reports it unchanged.

Data from Enhetsregisteret is published under the
[Norwegian Licence for Open Government Data (NLOD)](https://data.norge.no/nlod/en/2.0).
Attribute Brønnøysundregistrene when you republish it.

## Privacy and limits

- **No people.** Roles, directors, signatories, beneficial owners and person
  search are never requested. The authorized BRREG API is never called.
- **No contact details.** E-mail, mobile and telephone are never stored or
  published. When BRREG's patch says one of them changed, the row keeps the path
  and drops the value, flagged `value_redacted`.
- **No street lines.** Addresses are reduced to city and municipality code,
  including leaf operations and whole address objects in source patches,
  because a sole proprietorship's business address is often a home address.
- **`410 Gone` is honoured immediately.** The cached record is purged from state
  on the run that observes it, leaving only the organization number and the
  removal date.
- Public registry data about a sole proprietorship can still relate to a natural
  person. Handle downstream use accordingly.
- **These are registry observations, not verdicts.** `bankrupt: false` is what
  the register published at `scraped_at`, not a guarantee about a counterparty.
  Nothing here is a credit, AML, solvency or legal conclusion.

## FAQ

**Is this a free government API?** Yes. BRREG provides the underlying data for
free. The Actor price covers the saved watchlist, typed comparisons, reliable
recovery, billing limits and export workflow. It is an independent tool, not
affiliated with Brønnøysundregistrene.

**Can I search by company name?** This Actor accepts organization numbers. It
does not discover a lead list. Norwegian aliases are organisasjonsnummer,
Brønnøysundregistrene, Enhetsregisteret and overvåking av selskapsendringer.

**How do I recover after an interrupted run?** Ordinary failures preserve the
cursor. A hard process stop can leave a non-expiring monitor lock. Confirm the
old run has ended before clearing that monitor key’s lock request in the Actor’s
`norway-brreg-company-change-monitor-locks` queue. Do not start concurrent runs
under the same monitor key.

## Technical details

**Inputs**

| Field | How to use it |
| --- | --- |
| `organizationNumbers` | 1 to 5,000 nine-digit organization numbers, as strings or `{"organizationNumber": "..."}` objects. Spaces, dots, dashes and an `NO ... MVA` wrapper normalize away. A repeat is monitored, charged and compared once |
| `monitorKey` | 1 to 80 characters of letters, digits, `_` and `-`. Owns one baseline and one cursor. Two runs on the same key never overlap: the second fails rather than corrupting the cursor |
| `mode` | `changesOnly` (default) or `snapshotAndChanges` |
| `baselineMode` | `emitSnapshot` (default) or `storeOnly` |
| `includeSourcePatch` | Attach BRREG's own change operations as evidence. Default `true` |

**How a run works**

1. Capture the newest update id as the run's cursor start.
2. Look up every company that has no stored snapshot yet.
3. Capture the newest update id again as the run's cutoff.
4. Read the update stream over `[stored cursor + 1, cutoff]` for known companies,
   and over `[start + 1, cutoff]` for the ones just baselined. That second
   interval is the barrier that closes the race against the baseline.
5. Re-fetch the current record only for organizations that actually moved.
6. Diff, emit, then advance the cursor to the cutoff.

The run cutoff is an update id, not a timestamp. `updatedBefore` filters on the
event date while the cursor and sort key are the update id, so mixing them could
exclude an event inside the id interval and still advance past it. Pagination
walks by cursor for the same reason, and because BRREG caps `size * (page + 1)`
at 10,000.

**Measured on 2026-09-08** against the live API (developer workstation, so cloud
wall time differs):

| Basket | Source requests | Wall time |
| --- | --- | --- |
| 200-company first baseline | 202 | 3.8 s |
| 1,000-company first baseline | 1,002 | 15.0 s |
| 200-company daily check, nothing changed | 2 | 0.6 s |
| 1,000-company daily check | 16 | 1.3 s |
| 5,000-company hourly check | 27 | 1.8 s |

Watchlists are chunked at 200 organization numbers per update-stream request,
chosen by encoded URL length: 1,000 numbers (12,106 characters) still answered
`200` and 2,000 (24,106) was rejected.

**Permissions.** Limited permissions are sufficient: the Actor accesses its own
storages across runs. It does not request full account access.

**Storage.** One compact record per monitor key in the Actor's own key-value
store: the cursor and the last successful snapshot per organization. Large
delivery receipts use separate bounded KVS chunks, committed before the receipt
pointer. Old chunks are removed after their replacement is committed. Storage
capacity is checked before charging; oversized state or an oversized individual
row stops the run with the previous state preserved. No database.
Full observations live in each run's Dataset. Removing a company from the
watchlist removes its stored snapshot without inventing a registry event.

**Not in this Actor:** roles and officers, financial statements, bankruptcy
gazette notices, risk scores, lead enrichment, full-registry search or download,
and any country other than Norway.
