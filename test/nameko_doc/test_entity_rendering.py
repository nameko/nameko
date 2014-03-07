from mock import Mock, call
import pytest
from nameko.nameko_doc.extractors import entities


@pytest.fixture
def renderer():
    return Mock()


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
    assert renderer.mock_calls == [
        # The method call
        call.instruction(
            name='Foo', content='Bar'
        ),
        call.include_method(
            path='foo.services.example.ExampleService.method',
            no_index=True,
            extras=[renderer.instruction.return_value]
        ),
        call.title(
            text='Service Methods', level=2
        ),
        call.section(
            contents=[
                renderer.title.return_value,
                renderer.include_method.return_value
            ]
        ),
        # The see also section
        call.class_reference(
            path='blah.ServiceClass'
        ),
        call.definition(
            term='Service Class',
            description=renderer.class_reference.return_value,
        ),
        call.definition_list(
            contents=[renderer.definition.return_value]
        ),
        call.see_also_section(
            contents=[renderer.definition_list.return_value]
        ),
        # Page and title
        call.title(
            text='example', as_code=True, level=1
        ),
        call.include_module(
            path='foo.services.example'
        ),
        call.page(
            name='example',
            parts=[
                renderer.title.return_value,
                renderer.include_module.return_value,
                renderer.section.return_value,
                renderer.see_also_section.return_value,
            ]
        ),
        call.add_page(
            renderer.page.return_value
        )
    ]
