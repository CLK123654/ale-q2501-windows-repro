from __future__ import annotations

import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TASK = ROOT / "task"
expected = json.loads((ROOT / "qa/expected_hashes.json").read_text(encoding="utf-8"))
actual = {name: hashlib.sha256((TASK / name).read_bytes()).hexdigest() for name in expected}
if actual != expected:
    raise SystemExit("attachment hash mismatch")
(ROOT / "evidence").mkdir(exist_ok=True)
(ROOT / "evidence/attachment-hashes.json").write_text(json.dumps(actual, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
