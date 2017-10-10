"""A command-line interface for mongozen."""

import click

from .queries_cli import queries
from .util_cli import util


@click.group(help="A command-line interface for mongozen.")
def cli():
    """A command-line interface for mongozen."""
    pass


cli.add_command(queries)
cli.add_command(util)
