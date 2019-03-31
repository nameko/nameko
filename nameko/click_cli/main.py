import inspect
import os
import re
import sys
from functools import partial

import click
import six
import yaml

from nameko import config
from nameko.extensions import ENTRYPOINT_EXTENSIONS_ATTR

from .config import setup_config
from .run import main as main_run


MISSING_MODULE_TEMPLATE = "^No module named '?{}'?$"


def is_type(obj):
    return isinstance(obj, six.class_types)


def is_entrypoint(method):
    return hasattr(method, ENTRYPOINT_EXTENSIONS_ATTR)


def import_service(module_name, param=None, ctx=None):
    parts = module_name.split(":", 1)
    if len(parts) == 1:
        module_name, obj = module_name, None
    else:
        module_name, obj = parts[0], parts[1]

    try:
        __import__(module_name)
    except ImportError as exc:
        # ModuleNotFoundError does not exist prior python 3.6
        if exc.__class__.__name__ == "ModuleNotFoundError":
            raise click.BadParameter(exc.msg, param=param, ctx=ctx)
        if module_name.endswith(".py") and os.path.exists(module_name):
            raise click.BadParameter(
                "Failed to find service, did you mean '{}'?".format(
                    module_name[:-3].replace("/", ".")
                ),
                param=param,
                ctx=ctx,
            )

        missing_module_re = MISSING_MODULE_TEMPLATE.format(module_name)
        # is there a better way to do this?

        if re.match(missing_module_re, str(exc)):
            raise click.BadParameter(exc, param=param, ctx=ctx)

        # found module, but importing it raised an import error elsewhere
        # let this bubble (resulting in a full stacktrace being printed)
        raise

    module = sys.modules[module_name]

    if obj is None:
        found_services = []
        # find top-level objects with entrypoints
        for _, potential_service in inspect.getmembers(module, is_type):
            if inspect.getmembers(potential_service, is_entrypoint):
                found_services.append(potential_service)

        if not found_services:
            raise click.BadParameter(
                "Failed to find anything that looks like a service in module "
                "{!r}".format(module_name),
                param=param,
                ctx=ctx,
            )

    else:
        try:
            service_cls = getattr(module, obj)
        except AttributeError:
            raise click.BadParameter(
                "Failed to find service class {!r} in module {!r}".format(
                    obj, module_name
                ),
                param=param,
                ctx=ctx,
            )

        if not isinstance(service_cls, type):
            raise click.BadParameter("Service must be a class.", param=param, ctx=ctx)

        found_services = [service_cls]

    return found_services


class KeyValParamType(click.ParamType):
    """KEY=value formatted string. If only KEY is present, it gets value True.
    """

    name = "key_val"

    def convert(self, value, param, ctx):
        if "=" in value:
            key, val = value.split("=", 1)
            # TODO: make sure, we use properly imported yaml parser
            val = yaml.safe_load(val)
            return key.strip(), val
        else:
            return value.strip(), True


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


class NamekoModuleServicesParamType(click.ParamType):
    """Nameko service(s) specified in nameko compliant module.
    """

    name = "nameko_module_services"

    def convert(self, value, param, ctx):
        return import_service(value, param, ctx)


NAMEKO_MODULE_SERVICES = NamekoModuleServicesParamType()


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
