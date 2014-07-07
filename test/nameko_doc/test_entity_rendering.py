from mock import Mock, call
import pytest
from nameko.nameko_doc import entities
from nameko.nameko_doc.rst_render import RenderedPage


@pytest.fixture
def renderer():
    return Mock()


def test_entity_equality():
    one = entities.ExtraInstruction('foo', 'bar')
    two = entities.ExtraInstruction('foo', 'bar')
    assert one == two
    assert not (one != two)


def test_render_service_collection(renderer):
    service_collection = entities.ServiceCollection(services=[
        entities.ServiceDescription(
            'example',
            'foo.services.example',
            'ExampleService',
            sections=[
                entities.Section(
                    'Service Methods',
                    contents=[
                        entities.SingleMethod(
                            'method',
                            extras=[
                                entities.ExtraInstruction(
                                    title='Foo',
                                    content='Bar',
                                )
                            ]
                        )
                    ]
                ),
                entities.Section(
                    'Events',
                    contents=[
                        entities.SingleEvent(
                            'path.to.EventClass',
                            extras=[],
                        )
                    ]
                ),
                entities.ReferenceSection(
                    references=[
                        entities.ClassReference(
                            title='Service Class',
                            reference_path='blah.ServiceClass',
                        )
                    ]
                )
            ]
        )
    ])
    service_collection.render(renderer)

    expected_content = '\n'.join([
        '``example``',
        '===========',
        '',
        '.. automodule:: foo.services.example',
        '',
        'Service Methods',
        '---------------',
        '',
        '.. automethod:: foo.services.example.ExampleService.method',
        '    :noindex:',
        '    ',
        '    :Foo: Bar',
        '',
        'Events',
        '------',
        '',
        '.. autoclass:: path.to.EventClass',
        '    :noindex:',
        '',
        '.. seealso::',
        '',
        '    Service Class',
        '        :class:`blah.ServiceClass`',
        '    ',
        '',
    ])

    assert renderer.mock_calls == [
        call.add_page(
            RenderedPage(
                filename='example.rst',
                title='example',
                content=expected_content,
            )
        )
    ]
