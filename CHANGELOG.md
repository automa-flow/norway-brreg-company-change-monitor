# Changelog

## 2026-09-27: Fewer false change alerts and clearer missing-company results

### Fixes

- A company you add to the watchlist in a run that ends with a `PARTIAL` or
  budget-limited result no longer reports `OTHER_CHANGED` on the next run for
  register updates made before you started watching it.
- An organization number is reported as `NOT_FOUND` only when BRREG answers
  that it has no such record. When BRREG answers with a routing error instead,
  the company is reported as `SOURCE_FAILED`, it is not charged, and its stored
  state is kept.
- A BRREG update whose value differs from the current record only by
  surrounding spaces, or by an empty value, no longer keeps the company in
  `PARTIAL` on every run.
- A company removed from open data (HTTP 410) keeps the `REMOVED` record type
  on later `snapshotAndChanges` runs instead of appearing as `NOT_FOUND`.

Use `latest` to receive these fixes, or update a pinned build. Inputs, output
fields and prices are unchanged.

## 2026-09-16: Unrecognised input fields no longer fail the run (0.1.5)

### Fixes

- Top-level input fields the Actor does not recognise, for example from an
  older saved Task or an integration, are now ignored with a warning in the run
  log instead of failing the whole run.

## 2026-09-08: First public version (0.1.4)

### Features

- Watch up to 5,000 Norwegian organization numbers in BRREG Enhetsregisteret
  and get typed changes with before and after values: name, legal form,
  address, industry, employees, VAT registration, bankruptcy, liquidation,
  group membership, share capital, registration data and deletion.
- The first run stores a baseline. Later runs read BRREG's official update
  stream, so a quiet day for a large watchlist needs only a few requests.
- One row per watched company with an explicit status, so a company that could
  not be checked is never reported as unchanged.
