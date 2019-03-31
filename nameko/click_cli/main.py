from functools import partial

import click

from .paramtypes import HOST_PORT, KEY_VAL, NAMEKO_MODULE_SERVICES
from nameko import config

from .config import setup_config
from .run import main as main_run


def flatten_list_of_lists(ctx, param, list_of_lists):
    return sum(list_of_lists, [])


# CLI decorators. Allow reuse of unified opts/args whenever needed.
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
)
option_backdoor_port = partial(
    click.option,
    "--backdoor-port",
    help="Specify a port number to host a backdoor, which can be connected to"
    " for an interactive interpreter within"
    " the running service process using `nameko backdoor`.",
    type=HOST_PORT,
)
option_rlwrap = partial(
    click.option, "--rlwrap/--no-rlwrap", help="Use rlwrap", default=None
)
option_interface = partial(
    click.option, "--interface", type=click.Choice(["bpython", "ipython", "plain"])
)
argument_backdoor = partial(click.argument, "backdoor", type=HOST_PORT)
argument_services = partial(
    click.argument,
    "services",
    nargs=-1,
    metavar="module[:service class]",
    type=NAMEKO_MODULE_SERVICES,
    callback=flatten_list_of_lists,
)


# main nameko command
@click.group()
def main():
    """CLI to manage nameko based services and configuration.
    """
    pass


# nameko backdoor
@main.command()
@option_rlwrap()
@option_config_file()
@option_define()
@argument_backdoor()
def backdoor(config_file, define, backdoor, rlwrap):
    """Connect to a nameko backdoor. If a backdoor is running this will
    connect to a remote shell. The runner is generally available as `runner`.

    positional arguments:

      [host:]port           (host and) port to connect to
    """
    print(f"backdoor: {backdoor}")
    print(f"rlwrap: {rlwrap}")


# nameko show-config
@main.command()
@option_config_file()
@option_define()
def show_config(config_file, define):
    """Output as YAML string the configuration that would be passed to a service.
    Useful for viewing config files that load values from environement variables.
    """
    import yaml

    setup_config(config_file, define)
    click.echo(yaml.dump(config.data))


# nameko run
@main.command()
@option_broker()
@option_backdoor_port()
@option_config_file()
@option_define()
@argument_services()
def run(broker, config_file, define, backdoor_port, services):
    """Run nameko services. Given a python path to a module containing one or more
nameko services, will host and run them. By default this will try to find
classes that look like services (anything with nameko entrypoints), but a
specific service can be specified via ``nameko run module:ServiceClass``.

positional arguments:

  module[:service class]

        python path to one or more service classes to run
    """
    print(f"define: {define}")
    print(f"backdoor_port: {backdoor_port}")
    print(f"services: {services}")

    setup_config(config_file, define, broker)
    main_run(services, backdoor_port)


# nameko shell
@main.command()
@option_broker()
@option_interface()
@option_config_file()
@option_define()
def shell(broker, interface, config_file, define):
    print(f"interface: {interface}")
    from .shell import main

    setup_config(config_file, define, broker)
    main(interface)
