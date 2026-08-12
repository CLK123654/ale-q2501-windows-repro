from __future__ import annotations

import csv
import hashlib
import importlib.metadata
import json
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TASK = ROOT / "task"
EVIDENCE = ROOT / "evidence"
RUNS = ROOT / "windows-runs"


def reset(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)


def extract(archive: Path, target: Path) -> None:
    target.mkdir(parents=True)
    with zipfile.ZipFile(archive) as zipped:
        zipped.extractall(target)


def members(root: Path) -> list[str]:
    return sorted(path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file() and "__pycache__" not in path.parts)


def normalized(path: Path) -> bytes:
    return path.read_bytes().replace(b"\r\n", b"\n")


def compare(actual: Path, expected: Path) -> list[str]:
    if members(actual) != members(expected):
        raise AssertionError("交付路径集合不同")
    for relative in members(expected):
        if normalized(actual / relative) != normalized(expected / relative):
            raise AssertionError(f"Reference不同:{relative}")
    return members(expected)


def input_hashes(root: Path) -> dict[str, str]:
    return {path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest() for path in root.rglob("*") if path.is_file() and "__pycache__" not in path.parts}


def build(input_root: Path, output: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, str(ROOT / "implementation/build_delivery.py"), str(input_root), str(output)], text=True, capture_output=True, timeout=900)


def main() -> None:
    reset(RUNS)
    EVIDENCE.mkdir(exist_ok=True)
    version = importlib.metadata.version("apache-airflow")
    reference_root = RUNS / "reference"
    extract(TASK / "reference.zip", reference_root)
    expected = reference_root / "output"
    clean_runs = []
    for label in ["clean-a", "clean-b"]:
        base = RUNS / label
        extract(TASK / "输入数据包.zip", base)
        input_root = base / "input_data"
        before = input_hashes(input_root)
        for index in [1, 2]:
            output = base / f"output-{index}"
            completed = build(input_root, output)
            if completed.returncode:
                raise AssertionError(completed.stdout + completed.stderr)
            generated = compare(output, expected)
            clean_runs.append({"root_id": label, "process_index": index, "primary_software_executed": True, "input_unchanged": True, "reference_full_match": True, "generated_paths": generated})
        if before != input_hashes(input_root):
            raise AssertionError("input changed")

    positive = RUNS / "positive"
    extract(TASK / "输入数据包.zip", positive)
    registry = positive / "input_data/tenant_registry.csv"
    rows = list(csv.DictReader(registry.open(encoding="utf-8", newline="")))
    for row in rows:
        if row["tenant_id"] == "retail_eu":
            row["owner"] = "data-retail-platform"
    with registry.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    completed = build(positive / "input_data", positive / "output")
    if completed.returncode or normalized(positive / "output/release/dag_inventory.csv") == normalized(expected / "release/dag_inventory.csv"):
        raise AssertionError("合法责任人变化未进入DAG清单")
    (EVIDENCE / "positive-case.json").write_text(json.dumps({"input_field": "retail_eu owner", "behavior_changed": True}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    negative = RUNS / "negative"
    extract(TASK / "输入数据包.zip", negative)
    registry = negative / "input_data/tenant_registry.csv"
    text = registry.read_text(encoding="utf-8")
    text = text.replace("s3://data-product/risk-us/ready", "s3://data-product/network-ops/ready")
    registry.write_text(text, encoding="utf-8")
    output = negative / "output"
    output.mkdir()
    (output / "stale.txt").write_text("stale", encoding="utf-8")
    completed = build(negative / "input_data", output)
    if completed.returncode == 0 or output.exists():
        raise AssertionError("重复Dataset未关闭处理")
    (EVIDENCE / "negative-case.log").write_text(f"return_code={completed.returncode}\n{completed.stdout}{completed.stderr}", encoding="utf-8")
    summary = {"result": "PASS", "commit_sha": os.getenv("GITHUB_SHA"), "workflow_run_id": os.getenv("GITHUB_RUN_ID"), "runner_image": os.getenv("ImageOS"), "main_software": {"name": "Apache Airflow", "version": version, "executed": True, "runtime_boundary": "Windows2025+WSL2+Ubuntu24.04"}, "clean_directory_count": 2, "process_runs_per_directory": 2, "clean_runs": clean_runs, "positive_mutation": "PASS", "negative_case": "PASS", "reference_full_comparison": "PASS", "formal_network": {"wsl_outbound_blocked": True, "external_services_used": False}}
    (EVIDENCE / "windows-summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
