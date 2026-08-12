from __future__ import annotations

import importlib.metadata
import json
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT / "work-reference"
EVIDENCE = ROOT / "evidence"
if WORK.exists():
    shutil.rmtree(WORK)
WORK.mkdir()
with zipfile.ZipFile(ROOT / "task/输入数据包.zip") as archive:
    archive.extractall(WORK)
completed = subprocess.run(
    [sys.executable, str(ROOT / "implementation/build_delivery.py"), str(WORK / "input_data"), str(WORK / "output")],
    text=True,
    capture_output=True,
    timeout=300,
)
if completed.returncode:
    raise SystemExit(completed.stdout + completed.stderr)
EVIDENCE.mkdir(exist_ok=True)
with zipfile.ZipFile(EVIDENCE / "reference-candidate.zip", "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
    for path in sorted((WORK / "output").rglob("*")):
        if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc":
            archive.write(path, path.relative_to(WORK).as_posix())
(EVIDENCE / "reference-generation.json").write_text(
    json.dumps(
        {
            "result": "PASS",
            "commit_sha": os.getenv("GITHUB_SHA"),
            "workflow_run_id": os.getenv("GITHUB_RUN_ID"),
            "airflow_version": importlib.metadata.version("apache-airflow"),
            "qid": 2501,
        },
        indent=2,
    )
    + "\n",
    encoding="utf-8",
)
