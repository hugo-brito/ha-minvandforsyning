"""Water suppliers with confirmed MinVandforsyning supplier IDs."""

from __future__ import annotations

# Supplier ID -> supplier name. Add only IDs confirmed by a successful setup;
# suggestions arrive through the "Add a water supplier" GitHub issue form.
# Never add meter numbers or other personal data.
KNOWN_SUPPLIERS: dict[int, str] = {
    15: "Furesø Vandforsyning",
}
