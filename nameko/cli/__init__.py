import click

from nameko import config
from nameko.exceptions import CommandError, ConfigurationError

from .utils import setup_config
from .click_arguments import argument_services
from .click_options import (
    option_broker,
    option_config_file,
    option_define,
    option_backdoor_port,
    option_interface,
)


# main nameko command
@click.group()
@click.version_option()
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
@cli.command()
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

    from .run import main

    try:
        setup_config(config_file, define, broker)
        if backdoor_port:
            host, port = backdoor_port
            msg = "To connect to backdoor: `$ telnet {host} {port}`"
            click.echo(msg.format(host=host, port=port))
        main(services, backdoor_port)
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
