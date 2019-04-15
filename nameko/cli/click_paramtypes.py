"""Custom click parameter types used in nameko.
"""
import sys

import click
import six
import yaml

from .utils import import_services


class KeyValParamType(click.ParamType):
    """KEY=value formatted string. If only KEY is present, it gets value True.
    """

    name = "key_val"

    def convert(self, value, param, ctx):
        if not isinstance(value, six.string_types):
            self.fail("Value must by a string")
        if "=" in value:
            key, val = value.split("=", 1)
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

    def __init__(self, extra_sys_paths=None):
        """extra_sys_paths: list of paths, which are added to sys.path
        before iporting services.

        Note: syspath stays modified to preserve predictable environment
        for running the code.
        """
        self.extra_sys_paths = extra_sys_paths or []

    def convert(self, value, param, ctx):
        try:
            for path in self.extra_sys_paths:
                if path not in sys.path:
                    sys.path.insert(0, path)
            return import_services(value)
        except ValueError as exc:
            templ = "'{value}'. {msg}"
            args = {"value": value, "msg": str(exc)}
            self.fail(templ.format(**args), param, ctx)


NAMEKO_MODULE_SERVICES = NamekoModuleServicesParamType()
