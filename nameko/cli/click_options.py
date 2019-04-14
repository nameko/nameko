"""Custom click option decorators

Allow reuse of options across subcommands.

Reuse in other click based commands is also possible.

See nameko/cli/__init__.py for real use.
"""
from functools import partial

import click

from .click_paramtypes import HOST_PORT, KEY_VAL


option_broker = partial(
    click.option,
    "--broker",
    help="Deprecated option for setting up RabbitMQ broker URI.\n"
    "Use --define or --config and set AMQP_URI instead.",
    metavar="BROKER",
)
option_config_file = partial(
    click.option,
    "-c",
    "--config",
    "config_file",
    help="The YAML configuration file",
    type=click.File("rb"),
    metavar="CONFIG",
)
option_define = partial(
    click.option,
    "-d",
    "--define",
    help="Set config entry. Overrides value loaded from config file."
    " Can be used multiple times. Example: --define"
    " AMQP_URI=pyamqp://guest:guest@localhost",
    type=KEY_VAL,
    multiple=True,
    metavar="KEY=VALUE",
    callback=lambda ctx, param, value: dict(value),
)
option_backdoor_port = partial(
    click.option,
    "--backdoor-port",
    help="Specify a port number to host a backdoor, which can be connected to"
    " for an interactive interpreter within"
    " the running service process using `nameko backdoor`.",
    type=HOST_PORT,
)
option_interface = partial(
    click.option, "--interface", type=click.Choice(["bpython", "ipython", "plain"])
)
