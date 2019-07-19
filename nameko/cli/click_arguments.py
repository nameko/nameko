"""Custom click argument  decorators

Allow reuse of arguments across subcommands.

Reuse in other click based commands is also possible.

See nameko/cli/__init__.py for real use.
"""
from functools import partial

import click


argument_services = partial(
    click.argument,
    "services",
    nargs=-1,
    required=True,
    metavar="module[:service class]",
    type=str,
)
