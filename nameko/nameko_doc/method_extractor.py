import inspect
import logging

from nameko.containers import is_method
from nameko.rpc import Rpc
from . import entities


log = logging.getLogger(__name__)


def is_rpc_method(method):
    if is_method(method) and hasattr(method, 'nameko_entrypoints'):
        if any(isinstance(entrypoint, Rpc)
               for entrypoint in method.nameko_entrypoints):
            # It's an RPC method
            return True

    return False


class MethodExtractor(object):
    def __init__(self, service_loader_function):
        self.service_loader_function = service_loader_function

    def extract(self):
        descriptions = []

        services = self.service_loader_function()
        for service_cls in services:
            service_name = service_cls.name  # e.g. 'foo'
            service_class_name = service_cls.__name__  # e.g. 'FooService'
            module_path = service_cls.__module__
            method_names = self._get_service_method_names(service_cls)

            description = self._create_service_description(
                service_name,
                module_path,
                service_class_name,
                method_names,
            )
            descriptions.append(description)

        return entities.ServiceCollection(
            services=descriptions
        )

    def _get_service_method_names(self, service_cls):
        service_method_names = [
            name for (name, _) in
            inspect.getmembers(service_cls, is_rpc_method)
        ]
        return service_method_names

    def _create_service_description(self, service_name, module_path,
                                    service_cls_name, methods):
        reference_sections = [
            entities.ReferenceSection(
                references=[
                    entities.ClassReference(
                        title='Service Class',
                        reference_path='{}.{}'.format(
                            module_path, service_cls_name)
                    )
                ]
            )
        ]

        method_sections = [entities.Section(
            'RPC',
            contents=[
                entities.SingleMethod(
                    assigned_name,
                    extras=[]
                ) for assigned_name in methods
            ]
        )]

        all_sections = (
            method_sections + reference_sections
        )

        return entities.ServiceDescription(
            service_name,
            module_path,
            service_cls_name,
            sections=all_sections,
        )
