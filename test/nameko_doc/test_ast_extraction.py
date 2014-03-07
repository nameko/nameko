import ConfigParser
import pytest
from nameko.nameko_doc.extractors import AstExtractor, entities
from nameko.nameko_doc.extractors.ast_extractor import (
    CONFIG_SECTION, CONFIG_CLASS_SECTIONS, CONFIG_METHOD_SECTIONS)


class TestAstExtractor(object):
    @pytest.fixture
    def config_parser(self):
        config = ConfigParser.RawConfigParser()
        config.add_section(CONFIG_SECTION)
        config.set(CONFIG_SECTION, CONFIG_CLASS_SECTIONS, 'cls_instruct')
        config.set(CONFIG_SECTION, CONFIG_METHOD_SECTIONS, 'instruct')

        config.add_section('instruct')
        config.set('instruct', 'trigger_names',
                   'service_method, RpcProvider')
        config.set('instruct', 'title', 'Some Methods')

        config.add_section('cls_instruct')
        config.set('cls_instruct', 'trigger_names', 'service_class')

        return config

    def test_no_service_in_empty(self, copy_example, config_parser):
        source = copy_example('empty')
        with AstExtractor(source, config_parser) as e:
            res = e.extract()
        assert res == entities.ServiceCollection()

    def test_get_all_packages(self, copy_example, config_parser):
        source = copy_example('packages')
        with AstExtractor(source, config_parser) as e:
            all_packages = list(e.get_all_packages())
        assert all_packages == [
            'baz',
            'foo',
            'foo.bar',
        ]

    def test_process_simple(self, copy_example, config_parser):
        source = copy_example('recommended')
        with AstExtractor(source, config_parser) as e:
            processed = e.extract()
            service_cls_path = 'rec.services.example.ExampleService'
            assert processed == entities.ServiceCollection(services=[
                entities.ServiceDescription(
                    'example',
                    'rec.services.example',
                    'ExampleService',
                    sections=[
                        entities.Section(
                            'Some Methods',
                            contents=[
                                entities.SingleMethod(
                                    'method'
                                )
                            ]
                        ),
                        entities.ReferenceSection(
                            references=[
                                entities.ClassReference(
                                    title='Service Class',
                                    reference_path=service_cls_path
                                )
                            ]
                        )
                    ]
                )
            ])

    def test_process_from_controller(self, copy_example, config_parser):
        source = copy_example('recommended_with_controller')
        with AstExtractor(source, config_parser) as e:
            processed = e.extract()
            service_cls_path = 'with_controller.services.controller_example.' \
                               'controller.ExampleService'
            assert processed == entities.ServiceCollection(services=[
                entities.ServiceDescription(
                    'controller_example',
                    'with_controller.services.controller_example.controller',
                    'ExampleService',
                    sections=[
                        entities.Section(
                            'Some Methods',
                            contents=[
                                entities.SingleMethod(
                                    'method'
                                )
                            ]
                        ),
                        entities.ReferenceSection(
                            references=[
                                entities.ClassReference(
                                    title='Service Class',
                                    reference_path=service_cls_path
                                )
                            ]
                        )
                    ]
                )
            ])

    def test_process_from_nameko(self, copy_example, config_parser):
        source = copy_example('nameko_decorator')
        with AstExtractor(source, config_parser) as e:
            processed = e.extract()
            assert processed == entities.ServiceCollection(services=[
                entities.ServiceDescription(
                    'nameko_example',
                    'nameko_example',
                    'ExampleService',
                    sections=[
                        entities.Section(
                            'Some Methods',
                            contents=[
                                entities.SingleMethod(
                                    'method'
                                )
                            ]
                        ),
                        entities.ReferenceSection(
                            references=[
                                entities.ClassReference(
                                    title='Service Class',
                                    reference_path='nameko_example.'
                                                   'ExampleService'
                                )
                            ]
                        )
                    ]
                )
            ])
