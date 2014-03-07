from abc import ABCMeta, abstractmethod
from path import path


class Renderer(object):
    __metaclass__ = ABCMeta

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()

    @abstractmethod
    def flush(self):
        """ Flush all captured pages to output """

    @abstractmethod
    def instruction(self, name, content):
        """ Render an arbitrary in-line instruction with the given name and
        content """

    @abstractmethod
    def class_reference(self, path):
        """ Render a cross-reference to a class """

    @abstractmethod
    def title(self, text, level=1, as_code=False):
        """ Render heading text at the given level (lower numbers more
        prominent), optional presenting it as code/monospaced """

    @abstractmethod
    def include_method(self, path, no_index=False, extras=None):
        """ Render an instruction to include documentation for a method """

    @abstractmethod
    def section(self, contents):
        """ Render an abstract section with the given contents """

    @abstractmethod
    def definition(self, term, description):
        """ Render a single definition with the given term and definition """

    @abstractmethod
    def definition_list(self, contents):
        """ Render a definition list with the given items """

    @abstractmethod
    def see_also_section(self, contents):
        """ Render a 'See Also' section with the given contents """

    @abstractmethod
    def include_module(self, path):
        """ Render an instruction to include documentation for a module """

    @abstractmethod
    def page(self, name, parts):
        """ Create a rendered page with a given name and a list of rendered
        contents """

    @abstractmethod
    def add_page(self, page):
        """ Register a renderer page with this collection """


def indent(text, size=4):
    lines = text.split('\n')
    indent = ' ' * size
    indented = '\n'.join(
        indent + line
        for line in lines
    )
    return indented


class RstDirectoryRenderer(Renderer):
    def add_page(self, page):
        self.pages.append(page)

    def page(self, name, parts):
        return {
            'filename': '{}.rst'.format(name),
            'title': name,
            'content': '\n'.join(parts)
        }

    def include_module(self, path):
        return '.. automodule:: {}\n'.format(path)

    def see_also_section(self, contents):
        return '.. seealso::\n\n{}\n'.format(
            indent('\n'.join(contents))
        )

    def definition_list(self, contents):
        return '{}\n'.format('\n'.join(contents))

    def definition(self, term, description):
        return '{}\n{}'.format(term, indent(description))

    def section(self, contents):
        return '\n'.join(contents)

    LEVEL_UNDERLINES = '=-~^'

    def include_method(self, path, no_index=False, extras=None):
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

    def title(self, text, level=1, as_code=False):
        underline_char = self.LEVEL_UNDERLINES[level - 1]

        title_text = text if not as_code else '``{}``'.format(text)
        underline = underline_char * len(title_text)

        return '{}\n{}\n'.format(title_text, underline)

    def class_reference(self, path):
        return ':class:`{}`'.format(path)

    def instruction(self, name, content):
        return ':{}: {}'.format(name, content)

    def __init__(self, output, config_parser):
        self.output = output

        self.pages = []

    def _file_path(self, file_name):
        return str(self.output_path.joinpath(file_name))

    @property
    def output_path(self):
        return path(self.output)

    @property
    def sorted_pages(self):
        return sorted(self.pages, key=lambda p: p['title'])

    def flush(self):
        if not self.pages:
            return

        # index first
        with open(self._file_path('services.rst'), 'w') as f:
            f.write(self._index_contents())

        for page in self.pages:
            with open(self._file_path(page['filename']), 'w') as f:
                f.write(page['content'])

    def _index_contents(self):
        page_lines = indent('\n'.join(
            page['title'] for page in self.sorted_pages))
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
