#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OBLE_ROOT_DEFAULT="$ROOT/docs/oble"
OBLE_ROOT="${OBLE_SOURCE:-$OBLE_ROOT_DEFAULT}"

cd "$ROOT"

echo "[1/3] validate schema/example map"
python3 - <<'PY' "$OBLE_ROOT"
import json
from pathlib import Path
import sys

oble_root = Path(sys.argv[1])
schema_dir = oble_root / "schema"
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
python3 - <<'PY' "$OBLE_ROOT"
import json
from pathlib import Path
import sys

for path in sorted((Path(sys.argv[1]) / "examples").glob("*.json")):
    json.loads(path.read_text())
PY

echo "[3/3] validate OBLE docs cross-references"
python3 - <<'PY' "$OBLE_ROOT"
from pathlib import Path
import re
import sys

oble_root = Path(sys.argv[1])
docs = [
    oble_root / "README.md",
    oble_root / "schema" / "README.md",
    oble_root / "schema" / "validation.md",
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

if [[ "$OBLE_ROOT" == "$OBLE_ROOT_DEFAULT" ]]; then
  echo "Validated vendored OBLE snapshot at $OBLE_ROOT"
else
  echo "Validated external OBLE source at $OBLE_ROOT"
fi
echo "OBLE validation checks passed"
