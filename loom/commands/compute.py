"""Compute pipeline commands: lm compute run/list"""
from __future__ import annotations

import json
import os
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from loom.core.compute import ComputeError, load_pipelines, run_pipeline

console = Console()


def _repo_root() -> Path:
    return Path(os.environ.get("LOOM_ROOT", ".")).resolve()


@click.group()
def compute():
    """Run cost allocation and compute pipelines."""
    pass


@compute.command(name="list")
@click.option("--json", "as_json", is_flag=True, help="output as JSON")
def list_cmd(as_json):
    """List available compute pipelines."""
    root = _repo_root()
    pipelines = load_pipelines(root)

    if as_json:
        result = [
            {"name": name, "description": p.get("description", ""), "steps": len(p.get("steps", []))}
            for name, p in pipelines.items()
        ]
        click.echo(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if not pipelines:
        console.print("[dim]No pipelines defined in compute.yaml.[/dim]")
        return

    t = Table(show_header=True)
    t.add_column("Pipeline")
    t.add_column("Description")
    t.add_column("Steps")
    for name, p in pipelines.items():
        t.add_row(name, p.get("description", ""), str(len(p.get("steps", []))))
    console.print(t)


@compute.command()
@click.argument("pipeline")
@click.option("--period", "-p", required=True, help="year-month, e.g. 2026-05")
@click.option("--dry-run", is_flag=True, help="preview without writing")
@click.option("--json", "as_json", is_flag=True, help="output as JSON")
def run(pipeline, period, dry_run, as_json):
    """Run a compute pipeline for a given period."""
    root = _repo_root()
    try:
        result = run_pipeline(root, pipeline, period, dry_run=dry_run)
    except ComputeError as e:
        if as_json:
            click.echo(json.dumps({"error": str(e)}))
        else:
            console.print(f"[red]Error:[/red] {e}")
        raise SystemExit(1)

    if as_json:
        click.echo(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if dry_run:
        console.print(f"[yellow]Dry run:[/yellow] {result['generated_rows']} rows would be generated")
        if result.get("rows"):
            t = Table(show_header=True)
            for col in result["rows"][0].keys():
                t.add_column(col)
            for row in result["rows"]:
                t.add_row(*[str(v or "") for v in row.values()])
            console.print(t)
    else:
        console.print(f"[green]Done:[/green] {result['generated_rows']} rows generated for {period}")
