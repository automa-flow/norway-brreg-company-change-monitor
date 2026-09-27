# Fixtures

Sanitized captures of the public BRREG Enhetsregisteret open API, taken on
2026-09-08 by `experiments/norway-brreg-company-change-monitor/probe.py`.

Only large, publicly listed legal entities appear here (Equinor ASA, DNB), plus
organization numbers that the live update stream itself published as `Sletting`,
`Ny` or `Fjernet` events. The contact fields BRREG publishes - `telefon`,
`mobil`, `epostadresse`, `hjemmeside` - are stripped from the entity captures:
the Actor never reads them, and a fixture is not the place to start collecting
them.

`error_404_unknown_path.json` was captured on 2026-09-27 with one manual request
to a deliberately wrong path (`/enheterX/923609016`). It is the body BRREG sends
when it cannot route a request; an unknown organization number is answered with
the same status and an empty body.

CI never touches the live source. Re-capture by running the probe manually.
