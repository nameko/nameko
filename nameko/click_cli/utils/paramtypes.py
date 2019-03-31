import click
import yaml

from .import_services import import_services


class KeyValParamType(click.ParamType):
    """KEY=value formatted string. If only KEY is present, it gets value True.
    """

    name = "key_val"

    def convert(self, value, param, ctx):
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

    def convert(self, value, param, ctx):
        try:
            return import_services(value)
        except ValueError as exc:
            templ = "'{value}'. {msg}"
            args = {"value": value, "msg": str(exc)}
            self.fail(templ.format(**args), param, ctx)
        except ImportError:
            # Can we do better error message?
            raise


NAMEKO_MODULE_SERVICES = NamekoModuleServicesParamType()
