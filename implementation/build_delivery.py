from __future__ import annotations

import csv
import json
import os
import re
import shutil
import sys
from pathlib import Path

from airflow.models.dagbag import DagBag
from airflow.models.serialized_dag import SerializedDAG


IDENTIFIER = re.compile(r"[a-z][a-z0-9_]*")
PARSING_DAG_ID = "_AIRFLOW_PARSING_CONTEXT_DAG_ID"
PARSING_TASK_ID = "_AIRFLOW_PARSING_CONTEXT_TASK_ID"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def load_inputs(input_root: Path) -> tuple[list[dict], list[dict], dict, dict]:
    required = [
        "tenant_registry.csv",
        "stage_catalog.csv",
        "factory_policy.json",
        "release_request.json",
        "current_dag/tenant_factory.py",
    ]
    missing = [name for name in required if not (input_root / name).is_file()]
    if missing:
        raise ValueError(f"MISSING_INPUT:{','.join(missing)}")
    tenants = read_csv(input_root / "tenant_registry.csv")
    stages = read_csv(input_root / "stage_catalog.csv")
    policy = json.loads((input_root / "factory_policy.json").read_text(encoding="utf-8"))
    release = json.loads((input_root / "release_request.json").read_text(encoding="utf-8"))
    tenant_ids = [row["tenant_id"] for row in tenants]
    if not tenants or len(tenant_ids) != len(set(tenant_ids)):
        raise ValueError("INVALID_TENANT_REGISTRY")
    datasets = [row["dataset_uri"] for row in tenants]
    if len(datasets) != len(set(datasets)):
        raise ValueError("DUPLICATE_DATASET_URI")
    for row in tenants:
        if not IDENTIFIER.fullmatch(row["tenant_id"]):
            raise ValueError("INVALID_TENANT_ID")
        if not row["schedule"] or not row["timezone"] or not row["owner"] or not row["dataset_uri"]:
            raise ValueError("INCOMPLETE_TENANT_ROW")
    grouped: dict[str, list[dict]] = {tenant_id: [] for tenant_id in tenant_ids}
    for row in stages:
        if row["tenant_id"] not in grouped or not IDENTIFIER.fullmatch(row["task_id"]):
            raise ValueError("INVALID_STAGE_ROW")
        grouped[row["tenant_id"]].append(row)
    for tenant_id, rows in grouped.items():
        sequences = [int(row["sequence"]) for row in rows]
        task_ids = [row["task_id"] for row in rows]
        if not rows or sequences != list(range(1, len(rows) + 1)) or len(task_ids) != len(set(task_ids)):
            raise ValueError(f"INVALID_STAGE_SEQUENCE:{tenant_id}")
    if policy.get("dag_id_pattern") != "tenant_<tenant_id>_daily" or policy.get("start_date") != "2026-01-01":
        raise ValueError("INVALID_FACTORY_POLICY")
    required_release = {
        "change_window_start",
        "change_window_end",
        "affected_service",
        "rollout_mode",
        "observation_minutes",
        "observation_metrics",
        "rollback_condition",
        "rollback_command",
        "release_owner",
    }
    if not required_release.issubset(release):
        raise ValueError("INCOMPLETE_RELEASE_REQUEST")
    return tenants, stages, policy, release


def compose_config(tenants: list[dict], stages: list[dict], policy: dict) -> dict:
    by_tenant: dict[str, list[dict]] = {}
    for row in stages:
        by_tenant.setdefault(row["tenant_id"], []).append(row)
    items = []
    for row in tenants:
        items.append(
            {
                "tenant_id": row["tenant_id"],
                "schedule": row["schedule"],
                "timezone": row["timezone"],
                "owner": row["owner"],
                "dataset_uri": row["dataset_uri"],
                "tags": [item for item in row["tags"].split(";") if item],
                "stages": [item["task_id"] for item in by_tenant[row["tenant_id"]]],
            }
        )
    return {"start_date": policy["start_date"], "catchup": policy["catchup"], "tenants": items}


def dag_source() -> str:
    return '''from __future__ import annotations

import json
from pathlib import Path

import pendulum
from airflow import DAG, Dataset
from airflow.operators.empty import EmptyOperator
from airflow.utils.dag_parsing_context import get_parsing_context


CONFIG_PATH = Path(__file__).with_name("tenant_factory_config.json")


def load_config():
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def build_dag(spec, start_date, catchup):
    dag_id = f"tenant_{spec['tenant_id']}_daily"
    with DAG(
        dag_id=dag_id,
        start_date=pendulum.parse(start_date, tz=spec["timezone"]),
        schedule=spec["schedule"],
        catchup=catchup,
        default_args={"owner": spec["owner"]},
        tags=sorted(spec["tags"]),
    ) as dag:
        previous = EmptyOperator(task_id="start")
        for task_id in spec["stages"]:
            current = EmptyOperator(task_id=task_id)
            previous >> current
            previous = current
        finish = EmptyOperator(task_id="finish", outlets=[Dataset(spec["dataset_uri"])])
        previous >> finish
    return dag


config = load_config()
context = get_parsing_context()
for tenant in config["tenants"]:
    candidate_id = f"tenant_{tenant['tenant_id']}_daily"
    if context.dag_id is not None and context.dag_id != candidate_id:
        continue
    globals()[candidate_id] = build_dag(tenant, config["start_date"], config["catchup"])
'''


def parse_dags(dags: Path, target: str | None = None) -> DagBag:
    previous = {PARSING_DAG_ID: os.environ.get(PARSING_DAG_ID), PARSING_TASK_ID: os.environ.get(PARSING_TASK_ID)}
    if target is None:
        os.environ.pop(PARSING_DAG_ID, None)
        os.environ.pop(PARSING_TASK_ID, None)
    else:
        os.environ[PARSING_DAG_ID] = target
        os.environ.pop(PARSING_TASK_ID, None)
    try:
        return DagBag(dag_folder=str(dags), include_examples=False, safe_mode=False, collect_dags=True)
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def scrub_serialized(value):
    ignored = {"fileloc", "relative_fileloc", "last_parsed_time", "processor_subdir"}
    if isinstance(value, dict):
        return {key: scrub_serialized(item) for key, item in sorted(value.items()) if key not in ignored}
    if isinstance(value, list):
        return [scrub_serialized(item) for item in value]
    return value


def build(input_root: Path, output: Path) -> None:
    if output.exists():
        shutil.rmtree(output)
    output.mkdir(parents=True)
    tenants, stages, policy, release = load_inputs(input_root)
    config = compose_config(tenants, stages, policy)
    dags = output / "dags"
    dags.mkdir()
    (dags / "tenant_factory.py").write_text(dag_source(), encoding="utf-8")
    (dags / "tenant_factory_config.json").write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    bag = parse_dags(dags)
    expected_ids = sorted(f"tenant_{row['tenant_id']}_daily" for row in tenants)
    if bag.import_errors or sorted(bag.dags) != expected_ids:
        raise RuntimeError(f"DAG_IMPORT:{bag.import_errors}")
    tenant_map = {f"tenant_{row['tenant_id']}_daily": row for row in tenants}
    inventory = []
    targeted = []
    serialized_dir = output / "serialized"
    serialized_dir.mkdir()
    for dag_id in expected_ids:
        dag = bag.dags[dag_id]
        tenant = tenant_map[dag_id]
        chain = []
        current = "start"
        while True:
            chain.append(current)
            downstream = sorted(dag.get_task(current).downstream_task_ids)
            if not downstream:
                break
            if len(downstream) != 1:
                raise RuntimeError(f"BRANCHED_CHAIN:{dag_id}")
            current = downstream[0]
        outlet = [item.uri for item in dag.get_task("finish").outlets]
        if outlet != [tenant["dataset_uri"]]:
            raise RuntimeError(f"DATASET_MISMATCH:{dag_id}")
        inventory.append(
            {
                "dag_id": dag_id,
                "schedule": str(dag.schedule_interval),
                "timezone": tenant["timezone"],
                "owner": tenant["owner"],
                "dataset_uri": tenant["dataset_uri"],
                "tags": ";".join(sorted(dag.tags)),
                "task_chain": ">".join(chain),
                "task_count": len(dag.tasks),
            }
        )
        selected = parse_dags(dags, dag_id)
        if selected.import_errors or sorted(selected.dags) != [dag_id]:
            raise RuntimeError(f"TARGETED_PARSE:{dag_id}")
        targeted.append({"requested_dag_id": dag_id, "built_dag_ids": dag_id, "task_count": len(selected.dags[dag_id].tasks)})
        serialized = scrub_serialized(SerializedDAG.to_dict(dag))
        (serialized_dir / f"{dag_id}.json").write_text(json.dumps(serialized, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    results = output / "release"
    write_csv(results / "dag_inventory.csv", ["dag_id", "schedule", "timezone", "owner", "dataset_uri", "tags", "task_chain", "task_count"], inventory)
    write_csv(results / "targeted_parse_inventory.csv", ["requested_dag_id", "built_dag_ids", "task_count"], targeted)
    summary = {
        **release,
        "dag_ids": expected_ids,
        "published_datasets": [row["dataset_uri"] for row in inventory],
        "candidate_path": "dags/tenant_factory.py",
        "configuration_path": "dags/tenant_factory_config.json",
        "serialized_directory": "serialized",
    }
    (results / "release-summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output / "README.md").write_text(
        "# 租户日批DAG发布材料\n\n"
        "dags目录包含候选DAG工厂及随包配置。release目录保存DAG清单、定向解析清单和维护窗安排。serialized目录是Airflow2.10.5生成的序列化结构，供编排平台导入前核对。\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    try:
        build(Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve())
    except Exception as exc:
        target = Path(sys.argv[2]).resolve()
        if target.exists():
            shutil.rmtree(target)
        print(f"ERROR:{exc}", file=sys.stderr)
        raise SystemExit(1)
