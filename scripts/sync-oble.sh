#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE="${1:-}"

if [[ -z "$SOURCE" ]]; then
  echo "usage: bash scripts/sync-oble.sh /path/to/oble-repo" >&2
  exit 1
fi

SOURCE="$(cd "$SOURCE" && pwd)"
TARGET="$ROOT/docs/oble"

for required in "$SOURCE/spec" "$SOURCE/examples" "$SOURCE/schema" "$SOURCE/profiles" "$SOURCE/conformance"; do
  if [[ ! -d "$required" ]]; then
    echo "missing required directory in OBLE source: $required" >&2
    exit 1
  fi
done

mkdir -p "$TARGET/examples" "$TARGET/schema"

cp "$SOURCE/spec"/000*.md "$TARGET/"
cp "$SOURCE/spec"/introduction.md "$TARGET/"
cp "$SOURCE/spec"/glossary.md "$TARGET/"
cp "$SOURCE/examples"/* "$TARGET/examples/"
cp "$SOURCE/schema"/* "$TARGET/schema/"
cp "$SOURCE/profiles/profile-matrix.md" "$TARGET/"
cp "$SOURCE/profiles/import-boundary.md" "$TARGET/"
cp "$SOURCE/conformance/conformance-checklist.md" "$TARGET/"
cp "$SOURCE/conformance/heft-conformance.md" "$TARGET/"

python3 - <<'PY' "$TARGET"
from pathlib import Path
import sys

target = Path(sys.argv[1])

replacements = {
    target / "schema" / "README.md": {
        "../examples/": "examples/",
        "../scripts/validate-oble.sh": "../../../scripts/validate-oble.sh",
    },
    target / "schema" / "validation.md": {
        "../scripts/validate-oble.sh": "../../../scripts/validate-oble.sh",
        "  -s schema/book.schema.json \\\n  -d examples/core-book.json": "  -s docs/oble/schema/book.schema.json \\\n  -d docs/oble/examples/core-book.json",
        "  -s schema/entry.schema.json \\\n  -d examples/core-entry-posted.json": "  -s docs/oble/schema/entry.schema.json \\\n  -d docs/oble/examples/core-entry-posted.json",
        "  -s schema/reversal-pair.schema.json \\\n  -d examples/reversal-pair.json": "  -s docs/oble/schema/reversal-pair.schema.json \\\n  -d docs/oble/examples/reversal-pair.json",
        "  -s schema/counterparty-open-item.schema.json \\\n  -d examples/counterparty-open-item.json": "  -s docs/oble/schema/counterparty-open-item.schema.json \\\n  -d docs/oble/examples/counterparty-open-item.json",
    },
    target / "profile-matrix.md": {
        "../conformance/heft-conformance.md": "heft-conformance.md",
    },
}

for path, mapping in replacements.items():
    text = path.read_text()
    for old, new in mapping.items():
        text = text.replace(old, new)
    path.write_text(text)
PY

echo "Synced OBLE snapshot from $SOURCE into $TARGET"
