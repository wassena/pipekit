"""Example: track price changes through a discount pipeline using audit."""

from __future__ import annotations

from pipekit.audit import audit_field, audit_step, get_audit_log, strip_audit
from pipekit.pipeline import Pipeline
from pipekit.transforms import map_field

# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

products = [
    {"id": 1, "name": "Widget", "price": 100.0, "category": "tools"},
    {"id": 2, "name": "Gadget", "price": 250.0, "category": "electronics"},
    {"id": 3, "name": "Doohickey", "price": 40.0, "category": "tools"},
]


# ---------------------------------------------------------------------------
# Transformation steps
# ---------------------------------------------------------------------------

def apply_category_discount(records):
    """10 % off electronics, 5 % off everything else."""
    out = []
    for rec in records:
        rec = dict(rec)
        factor = 0.90 if rec.get("category") == "electronics" else 0.95
        rec["price"] = round(rec["price"] * factor, 2)
        out.append(rec)
    return out


def apply_bulk_discount(records):
    """Extra 5 % off items over £200 after category discount."""
    out = []
    for rec in records:
        rec = dict(rec)
        if rec["price"] > 200:
            rec["price"] = round(rec["price"] * 0.95, 2)
        out.append(rec)
    return out


# ---------------------------------------------------------------------------
# Build the audited pipeline
# ---------------------------------------------------------------------------

pipeline = Pipeline([
    audit_field("price", label="original_price"),
    apply_category_discount,
    audit_field("price", label="after_category_discount"),
    apply_bulk_discount,
    audit_step("final_state", fields=["id", "price"]),
])

# ---------------------------------------------------------------------------
# Run and inspect
# ---------------------------------------------------------------------------

results_with_audit = pipeline(products)

for rec in results_with_audit:
    print(f"\n{rec['name']} (id={rec['id']})")
    for entry in get_audit_log(rec):
        if "field" in entry:
            print(f"  [{entry['field']}] = {entry['value']}")
        else:
            print(f"  [{entry['step']}] snapshot = {entry['snapshot']}")
    print(f"  => final price: {rec['price']}")

# Strip audit before saving
clean_results = strip_audit(results_with_audit)
assert "_audit" not in clean_results[0]
print("\nClean output:", clean_results)
