"""A command-line interface for mongozen."""

import click

from .util_cli import util


@click.group(help="A command-line interface for mongozen.")
def cli():
    """A command-line interface for mongozen."""
    pass


cli.add_command(util)
