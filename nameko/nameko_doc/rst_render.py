from collections import namedtuple
from path import Path

SERVICE_INDEX_FILENAME = 'index.rst'

LEVEL_UNDERLINES = '=-~^'


def indent(text, size=4):
    lines = text.split('\n')
    indent = ' ' * size
    indented = '\n'.join(
        indent + line
        for line in lines
    )
    return indented


class RstPagePrinter(object):
    def __init__(self, output):
        self.output = output

        self.pages = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()

    def add_page(self, page):
        """ Register a renderer page with this collection """
        self.pages.append(page)

    def _file_path(self, file_name):
        return str(self.output_path.joinpath(file_name))

    @property
    def output_path(self):
        return Path(self.output)

    @property
    def sorted_pages(self):
        return sorted(self.pages, key=lambda p: p.title)

    def flush(self):
        """ Flush all captured pages to output """
        if not self.pages:
            return

        # index first
        with open(self._file_path(SERVICE_INDEX_FILENAME), 'w') as f:
            f.write(self._index_contents())

        for page in self.pages:
            with open(self._file_path(page.filename), 'w') as f:
                f.write(page.content)

    def _index_contents(self):
        page_lines = indent('\n'.join(
            page.title for page in self.sorted_pages))
        lines = [
            'Services',
            '========',
            '',
            '.. toctree::',
            '    :maxdepth: 3',
            '',
            page_lines
        ]
        return '\n'.join(lines)


RenderedPage = namedtuple('RenderedPage', ['filename', 'title', 'content'])


def render_page(name, parts):
    """ Create a rendered page with a given name and a list of rendered
    contents """
    return RenderedPage(
        filename='{}.rst'.format(name),
        title=name,
        content='\n'.join(parts)
    )


def render_include_module(path):
    """ Render an instruction to include documentation for a module """
    return '.. automodule:: {}\n'.format(path)


def render_see_also_section(contents):
    """ Render a 'See Also' section with the given contents """
    return '.. seealso::\n\n{}\n'.format(
        indent('\n'.join(contents))
    )


def render_definition_list(contents):
    """ Render a definition list with the given items """
    return '{}\n'.format('\n'.join(contents))


def render_definition(term, description):
    """ Render a single definition with the given term and definition """
    return '{}\n{}'.format(term, indent(description))


def render_section(contents):
    """ Render an abstract section with the given contents """
    return '\n'.join(contents)


def render_include_method(path, no_index=False, extras=None):
    """ Render an instruction to include documentation for a method """
    extras = extras or []

    extra_lines = []
    if no_index:
        extra_lines.append(':noindex:')

    if extras:
        extra_lines.append('')

    for extra in extras:
        extra_lines.append(extra)

    extra_text = '\n'.join(extra_lines)
    extra_indented_content = indent(extra_text)

    return '.. automethod:: {}\n{}\n'.format(path, extra_indented_content)


def render_title(text, level=1, as_code=False):
    """ Render heading text at the given level (lower numbers more
    prominent), optional presenting it as code/monospaced """
    underline_char = LEVEL_UNDERLINES[level - 1]

    title_text = text if not as_code else '``{}``'.format(text)
    underline = underline_char * len(title_text)

    return '{}\n{}\n'.format(title_text, underline)


def render_class_reference(path):
    """ Render a cross-reference to a class """
    return ':class:`{}`'.format(path)


def render_instruction(name, content):
    """ Render an arbitrary in-line instruction with the given name and
    content """
    return ':{}: {}'.format(name, content)


def render_include_class(path, no_index=False, extras=None):
    """ Render an instruction to include documentation for a class """
    extras = extras or []

    extra_lines = []
    if no_index:
        extra_lines.append(':noindex:')

    if extras:
        extra_lines.append('')

    for extra in extras:
        extra_lines.append(extra)

    extra_text = '\n'.join(extra_lines)
    extra_indented_content = indent(extra_text)

    return '.. autoclass:: {}\n{}\n'.format(path, extra_indented_content)
