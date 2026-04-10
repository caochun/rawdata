import click
from rawdata.commands.data import data
from rawdata.commands.init import init
from rawdata.commands.sync import sync


@click.group()
def cli():
    """rawdata — spreadsheet-as-database with Git sync."""
    pass


cli.add_command(data)
cli.add_command(init)
cli.add_command(sync)
