"""A simple command-line tool for mongozen."""

import pprint

import click

from mongozen.param_inference_maps import rebuild_all_maps
from mongozen.collection_cfg import rebuild_collection_cfg_files
from mongozen.shared import _mongozen_cfg


@click.group(help="Utility mongozen operations.")
def util():
    """Utility mongozen operations."""
    pass


@util.command(help="Rebuild parameter inference maps.")
def rebuildmaps():
    """Rebuild parameter inference maps."""
    rebuild_all_maps()


@util.command(help="Rebuild collection attributes.")
def rebuildattr():
    """Rebuild collection attributes."""
    rebuild_collection_cfg_files()


@util.command(help="Print mongozen's current configuration.")
def printcfg():
    """Print mongozen's current configuration."""
    pprint.pprint(_mongozen_cfg(), indent=1, width=10)
