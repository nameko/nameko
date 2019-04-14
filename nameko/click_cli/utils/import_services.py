"""Utility function `import_services` to import nameko services
from strings pointing either to modules (then provide all nameko services
present there) or to explicit nameko service class.

Note: If you want to load locally present python files as modules,
modify sys.path::

    import sys
    if not "." in sys.path:
        sys.path.insert(0, ".")
    services = import_services("my_service")  # load my_service.py

See also NamekoModuleServicesParamType if you want to use it with click.
"""
import inspect
import os
import re
import sys

import six

from nameko.extensions import ENTRYPOINT_EXTENSIONS_ATTR


MISSING_MODULE_TEMPLATE = "^No module named '?{}'?$"


def is_type(obj):
    return isinstance(obj, six.class_types)


def is_entrypoint(method):
    return hasattr(method, ENTRYPOINT_EXTENSIONS_ATTR)


def import_services(module_name):
    """import nameko services from specified module (with optional service name)

    `module_name` value can be in style:
    - 'amodule': all nameko services in module amodule
    - 'pck.amodule': all nameko services in module 'pck.amodule'
    - 'amodule:MySvc': 'MySvc' nameko class from amodule
    - 'pck.amodule:MySvc': 'MySvc' nameko service from module 'pck.amodule'

    Raises:

    - ValueError with explanatory message and possible hint. Covers scenarios
      such as: missing module, no nameko class found in the module, explicit
      nameko service class is missing, is not a class or is not nameko service.
    - ImportError when module exists, but importing raised unexpected exception.

    Returns: list of imported services
    """
    parts = module_name.split(":", 1)
    if len(parts) == 1:
        module_name, service_name = module_name, None
    else:
        module_name, service_name = parts[0], parts[1]

    try:
        __import__(module_name)
    except ImportError as exc:
        if module_name.endswith(".py") and os.path.exists(module_name):
            raise ValueError(
                "Failed to find service, did you mean '{}'?".format(
                    module_name[:-3].replace("/", ".")
                )
            )

        missing_module_re = MISSING_MODULE_TEMPLATE.format(module_name)
        # is there a better way to do this?

        if re.match(missing_module_re, str(exc)):
            raise ValueError(exc)

        # found module, but importing it raised an import error elsewhere
        # let this bubble (resulting in a full stacktrace being printed)
        raise

    module = sys.modules[module_name]

    if service_name is None:
        found_services = []
        # find top-level objects with entrypoints
        for _, potential_service in inspect.getmembers(module, is_type):
            if inspect.getmembers(potential_service, is_entrypoint):
                found_services.append(potential_service)

        if not found_services:
            raise ValueError(
                "Failed to find anything that looks like a service in module "
                "{!r}".format(module_name)
            )

    else:
        try:
            service_cls = getattr(module, service_name)
        except AttributeError:
            raise ValueError(
                "Failed to find service class {!r} in module {!r}".format(
                    service_name, module_name
                )
            )

        if not isinstance(service_cls, type):
            raise ValueError("Service must be a class.")

        found_services = [service_cls]

    return found_services
