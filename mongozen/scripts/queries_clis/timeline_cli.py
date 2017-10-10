"""A command-line interface for timeline-related MongoDB queries."""

import click

import mongozen.queries.timeline as tl_queries
from ..shared_options import ENV_OPTION

_SHARED_OPTIONS = [
    ENV_OPTION
]

def _shared_options(func):
    for option in reversed(_SHARED_OPTIONS):
        func = option(func)
    return func


@click.group(help="Timeline-related MongoDB queries.")
def timeline():
    """Timeline-related MongoDB queries."""
    pass


@timeline.command(help="Find timeline stats.")
@click.argument('from_dateint', type=int)
@click.argument('to_dateint', type=int)
@_shared_options
def stats(from_dateint, to_dateint, environment):
    """Find timeline stats."""
    tl_queries.timeline_stats(
        from_dateint=from_dateint, to_dateint=to_dateint, env_name=environment)
