from nameko.nameko_doc import entities
from nameko.nameko_doc.method_extractor import MethodExtractor

from test.nameko_doc.example import ExampleService, ExampleEvent


class TestMethodExtraction(object):
    def test_process_from_nameko(self):
        def service_loader():
            return [
                ExampleService,
            ]

        def event_loader(_):
            return [
                ExampleEvent,
            ]

        e = MethodExtractor(service_loader, event_loader)
        processed = e.extract()
        assert processed == entities.ServiceCollection(services=[
            entities.ServiceDescription(
                'nameko_example',
                'test.nameko_doc.example',
                'ExampleService',
                sections=[
                    entities.Section(
                        'RPC',
                        contents=[
                            entities.SingleMethod(
                                'method', extras=[]
                            )
                        ]
                    ),
                    entities.Section(
                        'Events',
                        contents=[
                            entities.SingleEvent(
                                'test.nameko_doc.example.ExampleEvent',
                                extras=[]
                            )
                        ]
                    ),
                    entities.ReferenceSection(
                        references=[
                            entities.ClassReference(
                                title='Service Class',
                                reference_path='test.nameko_doc.example.'
                                               'ExampleService'
                            )
                        ]
                    )
                ]
            )
        ])
