# Security policy

## Reporting a vulnerability

Please report security issues privately through a [GitHub security advisory](https://github.com/hugo-brito/ha-minvandforsyning/security/advisories/new), not a public issue. Include the affected version, a description, and steps to reproduce.

## Handling sensitive data

This integration reads a household water meter. Two values are sensitive:

- The meter number. It identifies a delivery point and can be used to query consumption history from the supplier API.
- The consumption trace. Hourly water use reveals occupancy patterns.

Rules for this repository:

- Never commit real API responses or real meter numbers, whether in code, tests, fixtures, logs, issues, or pull requests.
- Test fixtures must be synthetic. See [docs/testing-fixtures.md](docs/testing-fixtures.md).
- Redact the meter number when sharing logs.

If sensitive data reaches Git history, follow [docs/runbooks/sensitive-data-removal.md](docs/runbooks/sensitive-data-removal.md).
