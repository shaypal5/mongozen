"""A command-line interface for mongozen-powered MongoDB queries."""

import click

from .queries_clis import timeline


@click.group(
    help="mongozen-powered MongoDB queries.")
def queries():
    """Command-line interface for mongozen-powered MongoDB queries."""
    pass


queries.add_command(timeline)
