import inspect
import logging

import entities
from nameko.rpc import RpcProvider


log = logging.getLogger(__name__)


class MethodExtractor(object):
    def __init__(self, service_loader_function):
        self.service_loader_function = service_loader_function

    def extract(self):
        descriptions = []

        services = self.service_loader_function()
        for service_name, service_cls in services:
            module_path = service_cls.__module__
            methods = self._service_methods_from_class(service_cls)
            description = self._create_service_description(
                service_name,
                module_path,
                service_cls.__name__,
                methods,
            )
            descriptions.append(description)

        return entities.ServiceCollection(
            services=descriptions
        )

    def _service_methods_from_class(self, class_obj):
        service_methods = []

        cls_method_data = inspect.getmembers(class_obj, inspect.ismethod)
        for method_name, method in cls_method_data:
            match_data = self._handle_as_service_method(
                method_name, method)
            if match_data:
                service_methods.append(match_data)

        return service_methods

    def _handle_as_service_method(self, method_name, method):
        if hasattr(method, 'nameko_entrypoints'):
            entrypoint_dep_classes = {
                factory.dep_cls for factory in method.nameko_entrypoints
            }
            if any(issubclass(dep_cls, RpcProvider)
                   for dep_cls in entrypoint_dep_classes):
                # It's an RPC method
                match_data = {
                    'assigned_name': method_name,
                }
                return match_data

        return None

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

        method_names = [
            method_tree['assigned_name'] for method_tree in methods
        ]

        method_sections = [entities.Section(
            'RPC',
            contents=[
                entities.SingleMethod(
                    assigned_name
                ) for assigned_name in method_names
            ]
        )]

        return entities.ServiceDescription(
            service_name,
            module_path,
            service_cls_name,
            sections=method_sections + reference_sections
        )
