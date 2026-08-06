# Synthetic fixture generator

Generates `tests/fixtures/meter_data.bin` and `tests/fixtures/ground_truth.json`
from hand-authored synthetic parameters. No value here derives from a real
household. See [docs/testing-fixtures.md](../../../docs/testing-fixtures.md) for
the policy.

## What it produces

Two contiguous UTC hourly windows in table 6 (168 rows total):

- Autumn fall-back: 2025-10-24 to 2025-10-27, covering the 25-hour local day on
  2025-10-26.
- Spring-forward: 2026-03-28 to 2026-03-30, covering the 23-hour local day on
  2026-03-29, including the `02:00` UTC hour that does not exist in local time.

`Reading` is cumulative from the synthetic consumption profile, so
`Consumption(T) == (Reading(T) - Reading(T-1)) * 1000` holds exactly. The two
windows are joined by a single intentional time gap.

## Regenerate (offline, pinned - not run in CI)

It uses the native protobuf-net-data serializer, the same family the supplier
API uses, so the round-trip through the Python parser is not circular.

```powershell
cd tests/fixtures/generator
podman run --rm -v "${PWD}:/src:Z" -w /src mcr.microsoft.com/dotnet/sdk:9.0 sh -c "dotnet run -c Release"
Move-Item -Force meter_data.bin, ground_truth.json ..
```

Output is deterministic; verify against `PROVENANCE.txt`.
