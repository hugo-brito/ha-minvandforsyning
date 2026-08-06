# Testing and fixture policy

## No real API data in the repository

Committed fixtures must not contain real supplier responses, real meter numbers, or any data derived from a real household. Offsetting, date-shifting, adding noise to, or shuffling a real trace is not enough: the hourly consumption still fingerprints occupancy.

## Fixtures must be synthetic and deterministic

- Generate consumption from hand-authored parameters or fixed formulas with a fixed seed. Do not fit parameters to any real trace.
- Derive cumulative `Reading` values from the synthetic consumption so the invariant `Consumption(T) == (Reading(T) - Reading(T-1)) * 1000` holds exactly.
- Keep the committed binary fixture as small as the wire-format coverage allows. Build larger corpora in memory during the test rather than committing a household-shaped trace.
- Prefer an independent encoder for the protobuf-net-data binary, for example a pinned .NET `protobuf-net-data` reference generator, rather than the Python parser under test. That keeps the round-trip test from being circular.
- Record the generator version, culture, dependency versions, and artifact hashes next to the fixture.

## What fixtures can and cannot prove

- Public tests enforce the documented API contract only.
- A synthetic fixture cannot prove the supplier's real behavior. The empirical proof of UTC end-of-hour semantics (issue #9) came from live API probing and does not depend on committed fixtures.
- Any live check must be opt-in and local only, take the meter number from an environment variable, redact all output, and retain no response data. Never upload live payloads as CI artifacts.
