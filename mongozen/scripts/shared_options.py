"""Options that different mongozen CLI commands share."""

import click

ENV_OPTION = click.option(
    '--environment', '-env', default='production', type=str,
    help="Production, staging or performance. Default: production.")
