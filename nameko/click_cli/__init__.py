from functools import partial

import click

from nameko import config
from nameko.exceptions import CommandError, ConfigurationError

from .run import main as main_run
from .utils.config import setup_config
from .utils.paramtypes import HOST_PORT, KEY_VAL, NamekoModuleServicesParamType


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
    callback=lambda ctx, param, value: dict(value)
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


# main nameko command
@click.group()
def cli():
    """CLI to manage nameko based services and configuration.
    """
    pass


# nameko show-config
@cli.command()
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
@cli.command("run")
@option_broker()
@option_backdoor_port()
@option_config_file()
@option_define()
@argument_services()
# underscore run_ prevents function/module name conflict when patching
def run_(broker, config_file, define, backdoor_port, services):
    """Run nameko services. Given a python path to a module containing one or more
nameko services, will host and run them. By default this will try to find
classes that look like services (anything with nameko entrypoints), but a
specific service can be specified via ``nameko run module:ServiceClass``.

positional arguments:

  module[:service class]

        python path to one or more service classes to run
    """
    try:
        setup_config(config_file, define, broker)
        if backdoor_port:
            host, port = backdoor_port
            msg = "To connect to backdoor: `$ telnet {host} {port}`"
            click.echo(msg.format(host=host, port=port))
        main_run(services, backdoor_port)
    except (CommandError, ConfigurationError) as exc:
        click.echo("Error: {}".format(exc))
        raise click.Abort()


# nameko shell
@cli.command()
@option_broker()
@option_interface()
@option_config_file()
@option_define()
def shell(broker, interface, config_file, define):
    from .shell import main

    setup_config(config_file, define, broker)
    main(interface)
