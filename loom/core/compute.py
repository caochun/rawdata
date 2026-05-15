"""Compute / cost allocation engine — reads compute.yaml pipelines."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from loom.core.store import StoreError, read_table, write_table


class ComputeError(Exception):
    pass


def load_pipelines(repo_root: Path) -> dict:
    path = repo_root / "compute.yaml"
    if not path.exists():
        return {}
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return data.get("pipelines", {})


def run_pipeline(
    repo_root: Path,
    pipeline_name: str,
    year_month: str,
    dry_run: bool = False,
) -> dict:
    pipelines = load_pipelines(repo_root)
    if pipeline_name not in pipelines:
        raise ComputeError(f"Pipeline '{pipeline_name}' not found")

    pipeline = pipelines[pipeline_name]
    steps = pipeline.get("steps", [])
    context: dict[str, list[dict]] = {}
    generated_rows: list[dict] = []

    for step in steps:
        step_type = step.get("type")
        name = step.get("name", "unnamed")

        if step_type == "aggregate":
            result = _step_aggregate(repo_root, step, year_month, context)
            output_key = step.get("output", f"_{name}")
            context[output_key] = result

        elif step_type == "compute":
            _step_compute(step, context)

        elif step_type == "distribute":
            rows = _step_distribute(repo_root, step, year_month, context)
            generated_rows.extend(rows)

        else:
            raise ComputeError(f"Unknown step type: {step_type} in step '{name}'")

    if not dry_run and generated_rows:
        output_table = steps[-1].get("output", "")
        if output_table:
            existing = read_table(repo_root, output_table)
            existing = [
                r for r in existing
                if not (r.get("source") == "timesheet_alloc" and r.get("year_month") == year_month)
            ]
            existing.extend(generated_rows)
            write_table(repo_root, output_table, existing)

    return {
        "pipeline": pipeline_name,
        "year_month": year_month,
        "generated_rows": len(generated_rows),
        "dry_run": dry_run,
        "rows": generated_rows if dry_run else [],
    }


def _step_aggregate(
    repo_root: Path,
    step: dict,
    year_month: str,
    context: dict[str, list[dict]],
) -> list[dict]:
    source = step["source"]
    if source.startswith("_"):
        rows = context.get(source, [])
    else:
        rows = read_table(repo_root, source)

    date_col = step.get("date_filter_col", "year_month")
    if any(date_col in r for r in rows[:1]):
        rows = [r for r in rows if r.get(date_col, "").startswith(year_month)]

    group_by = step.get("group_by", [])
    agg = step.get("agg", {})

    if not group_by or not agg:
        return rows

    import pandas as pd
    df = pd.DataFrame(rows)
    if df.empty:
        return []

    for col, func in agg.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    agg_map = {}
    for col, func in agg.items():
        if col in df.columns:
            agg_map[col] = func

    grouped = df.groupby(group_by, dropna=False).agg(agg_map).reset_index()
    return grouped.fillna("").to_dict("records")


def _step_compute(step: dict, context: dict[str, list[dict]]) -> None:
    source_key = step["source"]
    rows = context.get(source_key, [])
    if not rows:
        return

    group_total = step.get("group_total")
    if group_total:
        field = group_total["field"]
        by = group_total["by"]
        alias = group_total["as"]

        totals: dict[str, float] = {}
        for r in rows:
            key = tuple(str(r.get(b, "")) for b in by)
            totals[key] = totals.get(key, 0) + float(r.get(field, 0) or 0)

        for r in rows:
            key = tuple(str(r.get(b, "")) for b in by)
            r[alias] = totals.get(key, 0)

    output_field = step.get("output_field")
    expression = step.get("expression", "")
    if output_field and expression:
        for r in rows:
            try:
                local_vars = {k: float(v) if v else 0 for k, v in r.items()
                              if isinstance(v, (int, float, str)) and _is_numeric(v)}
                r[output_field] = eval(expression, {"__builtins__": {}}, local_vars)
            except (ValueError, ZeroDivisionError, TypeError):
                r[output_field] = 0

    context[source_key] = rows


def _step_distribute(
    repo_root: Path,
    step: dict,
    year_month: str,
    context: dict[str, list[dict]],
) -> list[dict]:
    cost_source = step["cost_source"]
    ratio_source_key = step["ratio_source"]
    match_on = step.get("match_on", [])
    ratio_field = step.get("ratio_field", "ratio")
    amount_field = step.get("amount_field", "amount")
    output_fields = step.get("output_fields", {})

    if cost_source.startswith("_"):
        cost_rows = context.get(cost_source, [])
    else:
        cost_rows = read_table(repo_root, cost_source)
        cost_rows = [r for r in cost_rows if r.get("year_month", "") == year_month]

    ratio_rows = context.get(ratio_source_key, [])

    ratio_index: dict[tuple, list[dict]] = {}
    for rr in ratio_rows:
        key = tuple(str(rr.get(m, "")) for m in match_on)
        ratio_index.setdefault(key, []).append(rr)

    generated = []
    now = datetime.now(timezone.utc).isoformat()
    for cr in cost_rows:
        key = tuple(str(cr.get(m, "")) for m in match_on)
        matches = ratio_index.get(key, [])
        cost_amount = float(cr.get(amount_field, 0) or 0)

        for rr in matches:
            ratio = float(rr.get(ratio_field, 0) or 0)
            allocated = round(cost_amount * ratio, 2)

            row = {
                "id": str(uuid.uuid4()),
                "year_month": year_month,
                "amount": str(allocated),
                "created_at": now,
                "updated_at": now,
            }

            for out_key, mapping in output_fields.items():
                if mapping.startswith("from_ratio."):
                    src_field = mapping.split(".", 1)[1]
                    row[out_key] = rr.get(src_field, "")
                elif mapping.startswith("from_cost."):
                    src_field = mapping.split(".", 1)[1]
                    row[out_key] = cr.get(src_field, "")
                else:
                    row[out_key] = mapping

            generated.append(row)

    return generated


def _is_numeric(v: Any) -> bool:
    if isinstance(v, (int, float)):
        return True
    try:
        float(str(v))
        return True
    except (ValueError, TypeError):
        return False
