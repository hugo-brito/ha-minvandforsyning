"""Water suppliers with confirmed MinVandforsyning supplier IDs."""

from __future__ import annotations

# Supplier ID -> supplier name. Add only IDs confirmed by a successful setup;
# suggestions arrive through the "Add a water supplier" GitHub issue form.
# Setup shows names as Markdown, so copy each name as plain text from the
# supplier's own website. Never add meter numbers or other personal data.
KNOWN_SUPPLIERS: dict[int, str] = {
    15: "Furesø Vandforsyning",
}
