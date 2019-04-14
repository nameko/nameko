"""Custom click argument  decorators

Allow reuse of arguments across subcommands.

Reuse in other click based commands is also possible.

See nameko/cli/__init__.py for real use.
"""
from functools import partial

import click

from .click_paramtypes import NamekoModuleServicesParamType


def flatten_list_of_lists(ctx, param, list_of_lists):
    return sum(list_of_lists, [])


argument_services = partial(
    click.argument,
    "services",
    nargs=-1,
    required=True,
    metavar="module[:service class]",
    # allow import of modules from current dir (manipulate sys.path)
    type=NamekoModuleServicesParamType(extra_sys_paths=["."]),
    callback=flatten_list_of_lists,
)
