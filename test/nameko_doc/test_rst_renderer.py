from mock import Mock, patch, call, create_autospec
import pytest
from nameko.nameko_doc import rst_render as rst


@pytest.fixture
def renderer():
    return rst.RstPagePrinter(Mock())


def test_render_see_also():
    res = rst.render_see_also_section(
        contents=[
            rst.render_definition_list(
                contents=[
                    rst.render_definition(
                        term='Foo',
                        description='Bar'
                    ),
                    rst.render_definition(
                        term='Service Class',
                        description=rst.render_class_reference(
                            'nameko_doc.example_service.BarService',
                        )
                    )
                ]
            )
        ]
    ).strip()
    assert res == """
.. seealso::

    Foo
        Bar
    Service Class
        :class:`nameko_doc.example_service.BarService`""".strip()


def test_render_method():
    res = rst.render_include_method(
        path='foo.BarClass.method_name',
        no_index=True,
        extras=[
            rst.render_instruction(
                name='Listens to',
                content='a.thing',
            )
        ]
    ).strip()
    expected_lines = [
        '.. automethod:: foo.BarClass.method_name',
        '    :noindex:',
        '    ',
        '    :Listens to: a.thing',
    ]
    expected = '\n'.join(expected_lines)
    assert res == expected


def test_render_class():
    res = rst.render_include_class(
        path='foo.bar.events.EventClass',
        no_index=True,
        extras=[
            rst.render_instruction(
                name='Handled by',
                content='some.thing',
            )
        ]
    ).strip()
    expected_lines = [
        '.. autoclass:: foo.bar.events.EventClass',
        '    :noindex:',
        '    ',
        '    :Handled by: some.thing',
    ]
    expected = '\n'.join(expected_lines)
    assert res == expected


def test_render_section():
    res = rst.render_section(
        contents=[
            rst.render_title('Hello', level=1, as_code=True),
            rst.render_section(
                contents=[
                    rst.render_title('Bye', level=2),
                    rst.render_include_module(
                        path='hello.world',
                    ),
                    'Some Content',
                ]
            )
        ]
    ).strip()
    expected = """
``Hello``
=========

Bye
---

.. automodule:: hello.world

Some Content""".strip()
    assert res == expected


def test_render_page():
    res = rst.render_page(
        name='foo',
        parts=[
            rst.render_title('Three', level=3),
            rst.render_include_method(
                path='foo.BarClass.baz'
            )
        ]
    )
    expected_content_lines = [
        'Three',
        '~~~~~',
        '',
        '.. automethod:: foo.BarClass.baz',
        '    ',
        '',
    ]
    expected_content = '\n'.join(expected_content_lines)

    expected = rst.RenderedPage(
        filename='foo.rst',
        content=expected_content,
        title='foo'
    )

    assert res == expected


@pytest.yield_fixture
def mock_write():
    with open('/dev/null') as test:
        file_type = type(test)  # can we get this from six?
    with patch('nameko.nameko_doc.rst_render.open', create=True) as m:
        m.return_value = create_autospec(file_type)
        yield m, m.return_value.__enter__.return_value


def test_render_multiple_pages(renderer, mock_write):
    renderer.output = 'folder'
    mock_open, handle = mock_write

    with renderer:
        renderer.add_page(
            rst.RenderedPage(
                filename='foo.rst',
                content='Hello, World',
                title='foo',
            )
        )
        renderer.add_page(
            rst.RenderedPage(
                filename='bar.rst',
                content='Goodbye, World',
                title='bar',
            )
        )

    assert mock_open.call_args_list == [
        call('folder/index.rst', 'w'),
        call('folder/foo.rst', 'w'),
        call('folder/bar.rst', 'w'),
    ]

    assert handle.write.call_args_list == [
        call('Services\n========\n\n.. toctree::\n    :maxdepth: 3\n\n'
             '    bar\n    foo'),
        call('Hello, World'),
        call('Goodbye, World'),
    ]
