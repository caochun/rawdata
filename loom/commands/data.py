"""CRUD commands: lm data query/add/update/delete/status/commit"""
from __future__ import annotations

import json
import os
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from loom.core.schema import Schema, SchemaError
from loom.core.store import (
    aggregate_rows,
    apply_auto_fields,
    find_row,
    join_tables,
    query_rows,
    read_table,
    time_aggregate_rows,
    tree_ancestors,
    tree_descendants,
    tree_path,
    write_table,
)

console = Console()


def _repo_root() -> Path:
    return Path(os.environ.get("LOOM_ROOT", ".")).resolve()


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
@click.option("--date-col", help="date column for time-based aggregation")
@click.option("--period", help="time period: month, quarter, year (requires --date-col)")
@click.option("--json", "as_json", is_flag=True, help="output as JSON")
def stats(table, group_by, agg, date_col, period, as_json):
    """Aggregate statistics on TABLE."""
    root = _repo_root()
    rows = read_table(root, table)

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

    if date_col and period:
        result = time_aggregate_rows(
            rows, date_col=date_col, period=period,
            agg=parsed_agg, group_by=group_fields,
        )
    else:
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


@data.command(name="join")
@click.argument("left_table")
@click.argument("right_table")
@click.option("--on", "on_cols", help="join columns: left_col=right_col")
@click.option("--type", "join_type", default="inner",
              type=click.Choice(["inner", "left"]), help="join type")
@click.option("--filter", "-f", "filters", multiple=True,
              help="table.field=value filter")
@click.option("--fields", help="comma-separated columns to return (table.col format)")
@click.option("--limit", type=int, help="max rows")
@click.option("--json", "as_json", is_flag=True, help="output as JSON")
def join_cmd(left_table, right_table, on_cols, join_type, filters, fields, limit, as_json):
    """Join LEFT_TABLE with RIGHT_TABLE using catalog relationships."""
    from loom.core.catalog import Catalog
    root = _repo_root()
    catalog = Catalog.load(root)

    left_col = right_col = None
    if on_cols:
        if "=" not in on_cols:
            raise click.BadParameter("--on must be left_col=right_col")
        left_col, right_col = on_cols.split("=", 1)

    parsed_filters = {}
    for f in filters:
        if "=" not in f:
            raise click.BadParameter(f"filter must be table.field=value, got: {f}")
        k, v = f.split("=", 1)
        parsed_filters[k] = v

    field_list = [f.strip() for f in fields.split(",")] if fields else None

    result = join_tables(
        root, catalog, left_table, right_table,
        join_type=join_type,
        left_col=left_col, right_col=right_col,
        filters=parsed_filters or None,
        fields=field_list,
        limit=limit,
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
@click.option("--id", "node_id", required=True, help="node id to start from")
@click.option("--direction", default="down",
              type=click.Choice(["down", "up", "path"]),
              help="traversal direction")
@click.option("--parent-col", default="parent_id", help="parent column name")
@click.option("--json", "as_json", is_flag=True, help="output as JSON")
def tree(table, node_id, direction, parent_col, as_json):
    """Traverse tree structure in TABLE."""
    root = _repo_root()
    rows = read_table(root, table)

    if direction == "path":
        path_str = tree_path(rows, node_id, parent_col=parent_col)
        if as_json:
            click.echo(json.dumps({"path": path_str}))
        else:
            console.print(path_str)
        return

    if direction == "up":
        result = tree_ancestors(rows, node_id, parent_col=parent_col)
    else:
        result = tree_descendants(rows, node_id, parent_col=parent_col)

    if as_json:
        click.echo(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if not result:
        console.print("[dim]No nodes found.[/dim]")
        return

    t = Table(show_header=True)
    for col in result[0].keys():
        t.add_column(col)
    for row in result:
        t.add_row(*[str(v or "") for v in row.values()])
    console.print(t)


@data.command()
@click.option("--table", "-t", "tables", multiple=True,
              help="specific table(s) to validate (default: all)")
@click.option("--json", "as_json", is_flag=True, help="output as JSON")
def validate(tables, as_json):
    """Validate foreign key references across tables."""
    from loom.core.catalog import Catalog
    from loom.core.schema import validate_foreign_keys
    root = _repo_root()
    catalog = Catalog.load(root)

    errors = validate_foreign_keys(root, catalog, list(tables) or None)

    if as_json:
        click.echo(json.dumps(errors, ensure_ascii=False, indent=2))
        return

    if not errors:
        console.print("[green]All foreign key references are valid.[/green]")
        return

    for err in errors:
        console.print(
            f"  [red]FK violation:[/red] {err['table']}.{err['column']} "
            f"row={err['row_id']} ref={err['invalid_ref']} -> {err['ref_table']}"
        )
