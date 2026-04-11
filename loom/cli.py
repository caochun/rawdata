import click
from loom.commands.data import data
from loom.commands.init import init
from loom.commands.sync import sync


@click.group()
def cli():
    """loom — spreadsheet-as-database with Git sync."""
    pass


cli.add_command(data)
cli.add_command(init)
cli.add_command(sync)
