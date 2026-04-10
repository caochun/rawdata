"""Sync commands: rd sync run / conflicts / resolve"""
from __future__ import annotations

import json
from pathlib import Path

import click
from rich.console import Console

from rawdata.core.git_ops import GitError, push, sync as git_sync

console = Console()

# In-memory conflict store for the session.
# In a real implementation this would be a .rawdata/conflicts.json file.
_CONFLICTS_FILE = ".rawdata_conflicts.json"


def _repo_root() -> Path:
    return Path.cwd()


def _save_conflicts(root: Path, conflicts: list[dict]) -> None:
    path = root / _CONFLICTS_FILE
    path.write_text(json.dumps(conflicts, ensure_ascii=False, indent=2))


def _load_conflicts(root: Path) -> list[dict]:
    path = root / _CONFLICTS_FILE
    if not path.exists():
        return []
    return json.loads(path.read_text())


def _clear_conflicts(root: Path) -> None:
    path = root / _CONFLICTS_FILE
    if path.exists():
        path.unlink()


@click.group()
def sync():
    """Sync with remote and manage conflicts."""
    pass


@sync.command("run")
@click.option("--json", "as_json", is_flag=True)
def sync_run(as_json):
    """Commit pending changes, then pull remote and merge."""
    from rawdata.core.git_ops import commit_changes
    root = _repo_root()

    sha = commit_changes(root, "data: auto-commit before sync")
    if sha:
        console.print(f"[dim]Auto-committed pending changes ({sha})[/dim]")

    try:
        result = git_sync(root)
    except GitError as e:
        console.print(f"[red]Sync error:[/red] {e}")
        raise SystemExit(1)

    if result["status"] == "ok":
        _clear_conflicts(root)
        push(root)
        msg = {"status": "ok", "message": "Sync complete, no conflicts."}
    else:
        _save_conflicts(root, result["conflicts"])
        msg = {
            "status": "conflicts",
            "count": len(result["conflicts"]),
            "message": f"{len(result['conflicts'])} conflict(s) found. Run `rd sync conflicts` to review.",
        }

    if as_json:
        click.echo(json.dumps(msg, ensure_ascii=False, indent=2))
    else:
        if msg["status"] == "ok":
            console.print(f"[green]{msg['message']}[/green]")
        else:
            console.print(f"[yellow]{msg['message']}[/yellow]")


@sync.command()
@click.option("--json", "as_json", is_flag=True)
def conflicts(as_json):
    """List current merge conflicts."""
    root = _repo_root()
    items = _load_conflicts(root)

    if as_json:
        click.echo(json.dumps(items, ensure_ascii=False, indent=2))
        return

    if not items:
        console.print("[dim]No conflicts.[/dim]")
        return

    for c in items:
        console.print(
            f"[bold]{c['id']}[/bold]  "
            f"[dim]base=[/dim]{c['base']}  "
            f"[cyan]mine=[/cyan]{c['mine']}  "
            f"[magenta]theirs=[/magenta]{c['theirs']}"
        )


@sync.command()
@click.option("--id", "conflict_id", required=True, help="conflict id from `rd sync conflicts`")
@click.option("--value", required=True, help="resolved value to use")
@click.option("--json", "as_json", is_flag=True)
def resolve(conflict_id, value, as_json):
    """Resolve a conflict by choosing a value."""
    from rawdata.core.store import read_table, find_row, write_table
    from rawdata.core.git_ops import commit_changes

    root = _repo_root()
    items = _load_conflicts(root)

    target = next((c for c in items if c["id"] == conflict_id), None)
    if target is None:
        console.print(f"[red]Conflict not found:[/red] {conflict_id}")
        raise SystemExit(1)

    table = target["table"]
    rows = read_table(root, table)
    idx, row = find_row(rows, target["row_id"])
    if row is None:
        console.print(f"[red]Row not found:[/red] {target['row_id']}")
        raise SystemExit(1)

    rows[idx][target["field"]] = value
    write_table(root, table, rows)

    # remove resolved conflict
    remaining = [c for c in items if c["id"] != conflict_id]
    if remaining:
        _save_conflicts(root, remaining)
    else:
        _clear_conflicts(root)

    sha = commit_changes(root, f"resolve conflict {conflict_id} → {value}")
    result = {"ok": True, "conflict_id": conflict_id, "resolved_value": value, "commit": sha}

    if as_json:
        click.echo(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        console.print(
            f"[green]Resolved[/green] {conflict_id} = {value}  "
            f"commit={sha or '(uncommitted)'}"
        )
        if not remaining:
            console.print("[green]All conflicts resolved. Run `rd sync run` to push.[/green]")
