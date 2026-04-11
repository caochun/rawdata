"""CRUD commands: lm data query/add/update/delete/status/commit"""
from __future__ import annotations

import json
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from loom.core.schema import Schema, SchemaError
from loom.core.store import (
    aggregate_rows,
    apply_auto_fields,
    find_row,
    query_rows,
    read_table,
    write_table,
)

console = Console()


def _repo_root() -> Path:
    return Path.cwd()


@click.group()
def data():
    """Read and write table data."""
    pass


@data.command()
@click.argument("table")
@click.option("--filter", "-f", "filters", multiple=True,
              help="field=value filter, can repeat")
@click.option("--fields", help="comma-separated columns to return")
@click.option("--search", "-s", help="fuzzy search across all fields")
@click.option("--sort", "sort_by", help="sort by field (-field for descending)")
@click.option("--limit", type=int, help="max rows to return")
@click.option("--offset", type=int, help="skip first N rows")
@click.option("--json", "as_json", is_flag=True, help="output as JSON")
def query(table, filters, fields, search, sort_by, limit, offset, as_json):
    """Query rows from TABLE."""
    root = _repo_root()
    rows = read_table(root, table)

    parsed_filters = {}
    for f in filters:
        if "=" not in f:
            raise click.BadParameter(f"filter must be field=value, got: {f}")
        k, v = f.split("=", 1)
        parsed_filters[k] = v

    field_list = [f.strip() for f in fields.split(",")] if fields else None
    result = query_rows(
        rows,
        parsed_filters or None,
        field_list,
        search=search,
        sort_by=sort_by,
        limit=limit,
        offset=offset,
    )

    if as_json:
        click.echo(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if not result:
        console.print("[dim]No rows found.[/dim]")
        return

    t = Table(show_header=True)
    for col in result[0].keys():
        t.add_column(col)
    for row in result:
        t.add_row(*[str(v or "") for v in row.values()])
    console.print(t)


@data.command()
@click.argument("table")
@click.option("--data", "-d", "payload", required=True,
              help="JSON object of field values")
@click.option("--json", "as_json", is_flag=True)
def add(table, payload, as_json):
    """Add a row to TABLE (changes are staged, not committed)."""
    root = _repo_root()

    try:
        row = json.loads(payload)
    except json.JSONDecodeError as e:
        raise click.BadParameter(f"invalid JSON: {e}")

    try:
        schema = Schema.load(root)
        col_defs = schema.columns(table)
        row = apply_auto_fields(col_defs, row, is_new=True)
        errors = schema.validate_row(table, row)
        if errors:
            for err in errors:
                console.print(f"[red]Validation error:[/red] {err}")
            raise SystemExit(1)
    except SchemaError:
        pass

    rows = read_table(root, table)
    rows.append(row)
    write_table(root, table, rows)

    result = {"ok": True, "row": row}
    if as_json:
        click.echo(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        console.print(f"[green]Added[/green] id={row.get('id', '?')}  [dim](not committed — run lm data commit)[/dim]")


@data.command()
@click.argument("table")
@click.option("--id", "row_id", required=True, help="row id to update")
@click.option("--data", "-d", "payload", required=True,
              help="JSON object of fields to update")
@click.option("--json", "as_json", is_flag=True)
def update(table, row_id, payload, as_json):
    """Update a row in TABLE by id (changes are staged, not committed)."""
    root = _repo_root()

    try:
        patch = json.loads(payload)
    except json.JSONDecodeError as e:
        raise click.BadParameter(f"invalid JSON: {e}")

    rows = read_table(root, table)
    idx, existing = find_row(rows, row_id)
    if existing is None:
        console.print(f"[red]Row not found:[/red] id={row_id}")
        raise SystemExit(1)

    updated = {**existing, **patch}

    try:
        schema = Schema.load(root)
        col_defs = schema.columns(table)
        updated = apply_auto_fields(col_defs, updated, is_new=False)
        errors = schema.validate_row(table, updated)
        if errors:
            for err in errors:
                console.print(f"[red]Validation error:[/red] {err}")
            raise SystemExit(1)
    except SchemaError:
        pass

    rows[idx] = updated
    write_table(root, table, rows)

    result = {"ok": True, "row": updated}
    if as_json:
        click.echo(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        console.print(f"[green]Updated[/green] id={row_id}  [dim](not committed — run lm data commit)[/dim]")


@data.command()
@click.argument("table")
@click.option("--id", "row_id", required=True)
@click.option("--json", "as_json", is_flag=True)
def delete(table, row_id, as_json):
    """Delete a row from TABLE by id (changes are staged, not committed)."""
    root = _repo_root()
    rows = read_table(root, table)
    idx, existing = find_row(rows, row_id)
    if existing is None:
        console.print(f"[red]Row not found:[/red] id={row_id}")
        raise SystemExit(1)

    rows.pop(idx)
    write_table(root, table, rows)

    result = {"ok": True, "deleted_id": row_id}
    if as_json:
        click.echo(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        console.print(f"[green]Deleted[/green] id={row_id}  [dim](not committed — run lm data commit)[/dim]")


@data.command()
@click.option("--message", "-m", help="commit message (auto-generated if omitted)")
@click.option("--json", "as_json", is_flag=True)
def commit(message, as_json):
    """Commit all pending data changes."""
    from loom.core.git_ops import commit_changes
    root = _repo_root()
    msg = message or "data: save changes"
    sha = commit_changes(root, msg)
    if not sha:
        result = {"ok": True, "commit": None, "message": "Nothing to commit."}
    else:
        result = {"ok": True, "commit": sha, "message": f"Committed {sha}"}

    if as_json:
        click.echo(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        if sha:
            console.print(f"[green]Committed[/green] {sha}")
        else:
            console.print("[dim]Nothing to commit.[/dim]")


@data.command()
def status():
    """Show uncommitted changes in csv files."""
    import git
    root = _repo_root()
    try:
        repo = git.Repo(root)
    except git.InvalidGitRepositoryError:
        console.print("[yellow]Not a git repository.[/yellow]")
        return

    changed = [item.a_path for item in repo.index.diff(None)
               if item.a_path.endswith(".csv")]
    untracked = [f for f in repo.untracked_files if f.endswith(".csv")]

    if not changed and not untracked:
        console.print("[dim]No uncommitted csv changes.[/dim]")
        return

    for f in changed:
        console.print(f"  [yellow]modified:[/yellow] {f}")
    for f in untracked:
        console.print(f"  [green]new file:[/green] {f}")


@data.command()
@click.argument("table")
@click.option("--group-by", "-g", help="field(s) to group by (comma-separated)")
@click.option("--agg", "-a", required=True,
              help="aggregations: field=func,... (count, sum, avg, min, max)")
@click.option("--json", "as_json", is_flag=True, help="output as JSON")
def stats(table, group_by, agg, as_json):
    """Aggregate statistics on TABLE."""
    root = _repo_root()
    rows = read_table(root, table)

    # Parse --agg flag: "value=sum,id=count"
    parsed_agg = {}
    for pair in agg.split(","):
        if "=" not in pair:
            raise click.BadParameter(f"agg must be field=func, got: {pair}")
        col, func = pair.split("=", 1)
        parsed_agg[col.strip()] = func.strip()

    group_fields = (
        [g.strip() for g in group_by.split(",")]
        if group_by else None
    )

    result = aggregate_rows(rows, group_by=group_fields, agg=parsed_agg)

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
        t.add_row(*[str(v) for v in row.values()])
    console.print(t)
