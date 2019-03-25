from functools import partial

import click


class KeyValParamType(click.ParamType):
    """KEY=value formatted string
    """

    name = "key_val"

    def convert(self, value, param, ctx):
        if "=" not in value:
            self.fail("%s must include '=' such as 'KEY=value'" % value, param, ctx)
        key, val = value.split("=", 1)
        return key, val


KEY_VAL = KeyValParamType()


class HostPortParamType(click.ParamType):
    """[host]:port with default 'localhost' host. Port must be int.
    """

    name = "host_port"

    def convert(self, value, param, ctx):
        host = "localhost"
        port_str = ""
        if ":" in value:
            host, port_str = value.rsplit(":", 1)
        else:
            port_str = value
        try:
            port = int(port_str)
            return host, port
        except ValueError:
            self.fail("%s is not a valid port number" % port_str, param, ctx)


HOST_PORT = HostPortParamType()


# CLI decorators. Allow reuse of unified opts/args whenever needed.
option_broker = partial(
    click.option,
    "--broker",
    help="Deprecated option for setting up RabbitMQ broker URI.\n"
    "Use --define or --config and set AMQP_URI instead.",
)
option_config = partial(
    click.option,
    "-c",
    "--config",
    help="The YAML configuration file",
    type=click.File("rb"),
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
)
option_backdoor_port = partial(
    click.option,
    "--backdoor-port",
    help="Specify a port number to host a backdoor, which can be connected to"
    " for an interactive interpreter within"
    " the running service process using `nameko backdoor`.",
    type=HOST_PORT,
)
argument_backdoor = partial(click.argument, "backdoor", type=HOST_PORT)
option_rlwrap = partial(
    click.option, "--rlwrap/--no-rlwrap", help="Use rlwrap", default=None
)
option_interface = partial(
    click.option, "--interface", type=click.Choice(["bpython", "ipython", "plain"])
)


# main nameko command
@click.group()
@option_broker()
@option_config()
@option_define()
def main(broker, config, define):
    """CLI to manage nameko based services and configuration.
    """
    pass


# nameko backdoor
@main.command()
@option_rlwrap()
@option_config()
@option_define()
@argument_backdoor()
def backdoor(config, define, backdoor, rlwrap):
    """Connect to a nameko backdoor. If a backdoor is running this will
    connect to a remote shell. The runner is generally available as `runner`.

    positional arguments:

      [host:]port           (host and) port to connect to
    """
    print(f"backdoor: {backdoor}")
    print(f"rlwrap: {rlwrap}")


# nameko show-config
@main.command()
@option_config()
@option_define()
def show_config(config, define):
    """Output as YAML string the configuration that would be passed to a service.
    Useful for viewing config files that load values from environement variables.
    """
    print(f"config: {config}")
    print(f"define: {define}")


# nameko run
@main.command()
@option_broker()
@option_backdoor_port()
@option_config()
@option_define()
def run(broker, config, define, backdoor_port):
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


# nameko shell
@main.command()
@option_broker()
@option_interface()
@option_config()
@option_define()
def shell(broker, interface, config, define):
    print(f"interface: {interface}")
