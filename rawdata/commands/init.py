"""rd init — initialize a new data repo."""
from __future__ import annotations

import shutil
from pathlib import Path

import click
import git
from rich.console import Console

console = Console()

_EXAMPLE_DIR = Path(__file__).parent.parent.parent / "example"

_DEFAULT_SCHEMA = """\
tables:
  items:
    columns:
      id:
        type: uuid
        primary: true
        auto: true
      name:
        type: string
        required: true
      status:
        type: enum
        values: [active, inactive]
      created_at:
        type: datetime
        auto: true
    merge_strategy:
      status: last_write_wins
"""

_DEFAULT_CATALOG = """\
description: "Edit this file to describe your data in plain language for the agent."

tables:
  items: "Main data table."

relationships: []

notes: []
"""


@click.command()
@click.argument("path")
@click.option("--name", "-n", help="Short name / description for the dataset")
def init(path, name):
    """Initialize a new data repo at PATH."""
    target = Path(path).resolve()

    if target.exists() and any(target.iterdir()):
        console.print(f"[red]Error:[/red] {target} already exists and is not empty.")
        raise SystemExit(1)

    target.mkdir(parents=True, exist_ok=True)

    # Copy example files if they exist, otherwise use minimal defaults
    schema_src = _EXAMPLE_DIR / "schema.yaml"
    catalog_src = _EXAMPLE_DIR / "catalog.yaml"

    schema_dst = target / "schema.yaml"
    catalog_dst = target / "catalog.yaml"

    if schema_src.exists():
        shutil.copy(schema_src, schema_dst)
    else:
        schema_dst.write_text(_DEFAULT_SCHEMA)

    if catalog_src.exists():
        catalog_text = catalog_src.read_text()
        if name:
            catalog_text = catalog_text.replace(
                'description: "销售团队 CRM 数据，管理客户联系人和沟通记录"',
                f'description: "{name}"',
            )
        catalog_dst.write_text(catalog_text)
    else:
        catalog_dst.write_text(
            _DEFAULT_CATALOG if not name
            else _DEFAULT_CATALOG.replace(
                'description: "Edit this file to describe your data in plain language for the agent."',
                f'description: "{name}"',
            )
        )

    # .gitignore for the data repo
    (target / ".gitignore").write_text(".rawdata_conflicts.json\n")

    # Init git repo and first commit
    repo = git.Repo.init(target)
    repo.index.add(["schema.yaml", "catalog.yaml", ".gitignore"])
    repo.index.commit("init: rawdata repo")

    console.print(f"[green]Initialized[/green] data repo at {target}")
    console.print(f"  Edit [bold]schema.yaml[/bold] to define your tables")
    console.print(f"  Edit [bold]catalog.yaml[/bold] to describe your data for the agent")
    console.print(f"")
    console.print(f"  To use with Hermes:")
    console.print(f"    [dim]RAWDATA_ROOT={target} ./hermes.sh[/dim]")
    console.print(f"")
    console.print(f"  To push to GitHub:")
    console.print(f"    [dim]cd {target} && git remote add origin <url> && git push -u origin main[/dim]")
