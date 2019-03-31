import inspect
import os
import re
import sys

import click
import six
import yaml

from nameko.extensions import ENTRYPOINT_EXTENSIONS_ATTR


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
