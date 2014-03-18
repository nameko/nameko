from mock import Mock, patch, MagicMock, call
import pytest
from nameko.nameko_doc.rst_render import RstDirectoryRenderer


@pytest.fixture
def renderer():
    return RstDirectoryRenderer(Mock())


def test_render_see_also(renderer):
    res = renderer.see_also_section(
        contents=[
            renderer.definition_list(
                contents=[
                    renderer.definition(
                        term='Foo',
                        description='Bar'
                    ),
                    renderer.definition(
                        term='Service Class',
                        description=renderer.class_reference(
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


def test_render_method(renderer):
    res = renderer.include_method(
        path='foo.BarClass.method_name',
        no_index=True,
        extras=[
            renderer.instruction(
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


def test_render_section(renderer):
    res = renderer.section(
        contents=[
            renderer.title('Hello', level=1, as_code=True),
            renderer.section(
                contents=[
                    renderer.title('Bye', level=2),
                    renderer.include_module(
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


def test_render_page(renderer):
    res = renderer.page(
        name='foo',
        parts=[
            renderer.title('Three', level=3),
            renderer.include_method(
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
    assert res == {
        'filename': 'foo.rst',
        'content': expected_content,
        'title': 'foo',
    }


@pytest.yield_fixture
def mock_write():
    with patch('nameko.nameko_doc.rst_render.open', create=True) as m:
        m.return_value = MagicMock(spec=file)
        yield m, m.return_value.__enter__.return_value


def test_render_multiple_pages(renderer, mock_write):
    renderer.output = 'folder'
    mock_open, handle = mock_write

    with renderer:
        renderer.add_page(
            {'filename': 'foo.rst', 'content': 'Hello, World', 'title': 'foo'}
        )
        renderer.add_page(
            {'filename': 'bar.rst', 'content': 'Goodbye, World',
             'title': 'bar'},
        )

    assert mock_open.call_args_list == [
        call('folder/services.rst', 'w'),
        call('folder/foo.rst', 'w'),
        call('folder/bar.rst', 'w'),
    ]

    assert handle.write.call_args_list == [
        call('Services\n========\n\n.. toctree::\n    :maxdepth: 3\n\n'
             '    bar\n    foo'),
        call('Hello, World'),
        call('Goodbye, World'),
    ]
