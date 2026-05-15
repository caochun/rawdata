"""View commands: lm view query/list"""
from __future__ import annotations

import json
import os
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from loom.core.views import ViewError, load_views, run_view

console = Console()


def _repo_root() -> Path:
    return Path(os.environ.get("LOOM_ROOT", ".")).resolve()


@click.group()
def view():
    """Query predefined analysis views."""
    pass


@view.command(name="list")
@click.option("--json", "as_json", is_flag=True, help="output as JSON")
def list_cmd(as_json):
    """List available views."""
    root = _repo_root()
    views = load_views(root)

    if as_json:
        result = [
            {"name": name, "description": v.get("description", ""), "steps": len(v.get("steps", []))}
            for name, v in views.items()
        ]
        click.echo(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if not views:
        console.print("[dim]No views defined in views.yaml.[/dim]")
        return

    t = Table(show_header=True)
    t.add_column("View")
    t.add_column("Description")
    t.add_column("Steps")
    for name, v in views.items():
        t.add_row(name, v.get("description", ""), str(len(v.get("steps", []))))
    console.print(t)


@view.command(name="query")
@click.argument("name")
@click.option("--param", "-p", "params", multiple=True,
              help="view parameters: key=value")
@click.option("--limit", type=int, help="limit output rows")
@click.option("--json", "as_json", is_flag=True, help="output as JSON")
def query_cmd(name, params, limit, as_json):
    """Execute a predefined view by NAME."""
    root = _repo_root()

    parsed_params = {}
    for p in params:
        if "=" not in p:
            raise click.BadParameter(f"param must be key=value, got: {p}")
        k, v = p.split("=", 1)
        parsed_params[k] = v

    try:
        result = run_view(root, name, parsed_params or None)
    except ViewError as e:
        if as_json:
            click.echo(json.dumps({"error": str(e)}))
        else:
            console.print(f"[red]Error:[/red] {e}")
        raise SystemExit(1)

    if limit:
        result = result[:limit]

    if as_json:
        click.echo(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if not result:
        console.print("[dim]No data.[/dim]")
        return

    t = Table(show_header=True)
    for col in result[0].keys():
        t.add_column(col)
    for row in result:
        t.add_row(*[str(v or "") for v in row.values()])
    console.print(t)
