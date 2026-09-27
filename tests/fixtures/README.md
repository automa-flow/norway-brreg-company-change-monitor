# Fixtures

Sanitized captures of the public BRREG Enhetsregisteret open API, taken on
2026-09-08 by `experiments/norway-brreg-company-change-monitor/probe.py`.

Only large, publicly listed legal entities appear here (Equinor ASA, DNB), plus
organization numbers that the live update stream itself published as `Sletting`,
`Ny` or `Fjernet` events. The contact fields BRREG publishes - `telefon`,
`mobil`, `epostadresse`, `hjemmeside` - are stripped from the entity captures:
the Actor never reads them, and a fixture is not the place to start collecting
them.

CI never touches the live source. Re-capture by running the probe manually.
