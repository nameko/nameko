from collections import defaultdict
import importlib
import logging
from pkgutil import walk_packages
from path import path
import sys

import entities
from .ast_inspection import get_parsed_root_for_loaded_package

CONFIG_METHOD_SECTIONS = 'method_instructions'

CONFIG_CLASS_SECTIONS = 'class_instructions'

CONFIG_SECTION = 'ast_extract'


log = logging.getLogger(__name__)


class AstExtractor(object):
    def __init__(self, source, config_parser):
        self.source = path(source)

        self.service_class_triggers = set()
        self.service_method_triggers = set()
        self.title_for_method_trigger = {}
        self.order_index_for_method_trigger = {}
        self.interpreter_class_path = (
            'nameko.nameko_doc.extractors.ast_inspection.'
            'AstDecoratorBasedInterpreter'
        )

        self._process_config(config_parser)

        self._original_sys_path = None

    def __enter__(self):
        self._original_sys_path = [p for p in sys.path]
        sys.path.append(self.source)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.path = self._original_sys_path

    def _process_config(self, config_parser):
        self.service_class_triggers = self._get_trigger_names(
            config_parser.get(CONFIG_SECTION, CONFIG_CLASS_SECTIONS),
            config_parser
        )
        method_sections = config_parser.get(CONFIG_SECTION,
                                            CONFIG_METHOD_SECTIONS)
        self.service_method_triggers = self._get_trigger_names(
            method_sections,
            config_parser
        )

        sections = method_sections.split(',')
        index = 0
        for section in sections:
            index += 1
            names = config_parser.get(section, 'trigger_names').split(',')
            title = config_parser.get(section, 'title')
            for name in (n.strip() for n in names):
                if name not in self.title_for_method_trigger:
                    self.title_for_method_trigger[name] = title
                    self.order_index_for_method_trigger[name] = index

    def _get_trigger_names(self, section_list, config_parser):
        res = set()
        sections = section_list.split(',')
        for section in (s.strip() for s in sections):
            names = config_parser.get(section, 'trigger_names').split(',')
            res.update(n.strip() for n in names)
        return res

    def _get_interpreter_cls(self):
        mod_name, cls_name = self.interpreter_class_path.rsplit('.', 1)
        mod = importlib.import_module(mod_name)
        return getattr(mod, cls_name)

    def _get_interpreter(self):
        interpreter = self._get_interpreter_cls()(
            service_class_decorators=self.service_class_triggers,
            service_method_decorators=self.service_method_triggers,
        )
        return interpreter

    def get_service_name(self, fq_name):
        fq_name_parts = fq_name.split('.')
        if 'services' in fq_name_parts:
            service_name = fq_name_parts[fq_name_parts.index('services') + 1]
        else:
            service_name = fq_name_parts[-1]
        return service_name

    def extract(self):
        interpreter = self._get_interpreter()
        self.before_loading()

        packages = self.get_all_packages()
        services = []
        for fq_name in packages:
            module, tree = get_parsed_root_for_loaded_package(
                fq_name
            )

            processed = interpreter.process_services(module, tree)

            if processed:
                service_name = self.get_service_name(fq_name)

                service = self._to_service(
                    service_name,
                    fq_name,
                    processed
                )
                services.append(service)

        return entities.ServiceCollection(
            services=services
        )

    def before_loading(self):
        pass

    def get_all_packages(self):
        return self._yield_package(self.source)

    def _yield_package(self, search_path):
        for importer, fq_name, is_pkg in walk_packages([search_path]):
            package_name = fq_name.split('.')[-1]
            if is_pkg or package_name == 'controller':
                yield fq_name

    def _to_service(self, service_name, module_path, processed):
        log.debug('{} {} {}'.format(
            service_name, module_path, processed
        ))

        # Only one service per package is supported.
        assert len(processed) == 1
        service_cls_name, definition = processed.popitem()

        method_trees = definition['methods']

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

        partitioned = defaultdict(lambda: defaultdict(list))
        for method_tree in method_trees:
            assigned_name = method_tree['assigned_name']
            trigger = method_tree['trigger_name']
            index = self.order_index_for_method_trigger[trigger]
            title = self.title_for_method_trigger[trigger]

            partitioned[index][title].append(
                assigned_name
            )

        method_sections = []
        for i in sorted(partitioned.keys()):
            for title in sorted(partitioned[i].keys()):
                section = entities.Section(
                    title,
                    contents=[
                        entities.SingleMethod(
                            assigned_name
                        ) for assigned_name in partitioned[i][title]
                    ]
                )
                method_sections.append(section)

        return entities.ServiceDescription(
            service_name,
            module_path,
            service_cls_name,
            sections=method_sections + reference_sections
        )
