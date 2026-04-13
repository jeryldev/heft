#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT"

echo "[1/3] validate schema/example map"
python3 - <<'PY'
import json
from pathlib import Path

root = Path(".")
schema_dir = root / "docs/oble/schema"
mapping = json.loads((schema_dir / "example-map.json").read_text())["mappings"]

for item in mapping:
    schema_path = schema_dir / item["schema"]
    example_path = schema_dir / item["example"]
    if not schema_path.exists():
        raise SystemExit(f"missing schema: {schema_path}")
    if not example_path.exists():
        raise SystemExit(f"missing example: {example_path}")

    json.loads(schema_path.read_text())
    json.loads(example_path.read_text())
PY

echo "[2/3] parse all OBLE examples as JSON"
python3 - <<'PY'
import json
from pathlib import Path

for path in sorted(Path("docs/oble/examples").glob("*.json")):
    json.loads(path.read_text())
PY

echo "[3/3] validate OBLE docs cross-references"
python3 - <<'PY'
from pathlib import Path
import re

docs = [
    Path("docs/oble/README.md"),
    Path("docs/oble/schema/README.md"),
    Path("docs/oble/schema/validation.md"),
]
pattern = re.compile(r"\[[^\]]+\]\(([^)]+)\)")

for doc in docs:
    text = doc.read_text()
    for match in pattern.finditer(text):
        target = match.group(1)
        if target.startswith(("http://", "https://", "#")):
            continue
        resolved = (doc.parent / target).resolve()
        if not resolved.exists():
            raise SystemExit(f"broken link in {doc}: {target}")
PY

echo "OBLE validation checks passed"
