from collections import namedtuple
from . import rst_render as rst


TITLE_LEVEL_FOR_SUBSECTIONS = 2


class ServiceCollection(namedtuple('ServiceCollection', [
    'services',
])):
    def render(self, rst_page_printer):
        """ Render this service collection out """
        for service in self.services:
            page = service.render_page()
            rst_page_printer.add_page(page)


class ServiceDescription(namedtuple('ServiceDescription', [
    'name', 'module_path', 'class_name', 'sections',
])):
    def render_page(self):
        """ Render a service into a page """
        section_parts = [
            section.render_section(self, TITLE_LEVEL_FOR_SUBSECTIONS)
            for section in self.sections
        ]

        page = rst.render_page(
            name=self.name,
            parts=[
                rst.render_title(
                    text=self.name,
                    as_code=True,
                    level=1,
                ),
                rst.render_include_module(
                    path=self.module_path,
                )
            ] + section_parts
        )
        return page


class ReferenceSection(namedtuple('ReferenceSection', [
    'references',
])):
    def render_section(self, service_description, this_level=1):
        aside = rst.render_see_also_section(
            contents=[
                rst.render_definition_list(
                    contents=[
                        ref.render_reference()
                        for ref in self.references
                    ]
                )
            ]
        )
        return aside


class ClassReference(namedtuple('ClassReference', [
    'title', 'reference_path',
])):
    def render_reference(self):
        """ Render a reference to a class under a given category/title """
        return rst.render_definition(
            term=self.title,
            description=rst.render_class_reference(
                path=self.reference_path
            )
        )


class Section(namedtuple('Section', [
    'title', 'contents',
])):
    def render_section(self, service_description, this_level=1):
        sub_section_contents = [
            content.render_section(service_description, this_level + 1)
            for content in self.contents
        ]
        section = rst.render_section(
            contents=[
                rst.render_title(
                    text=self.title,
                    level=this_level,
                )
            ] + sub_section_contents
        )
        return section


class SingleMethod(namedtuple('SingleMethod', [
    'method_name', 'extras',
])):
    def render_section(self, service_description, this_level=1):
        method_path = '{}.{}.{}'.format(
            service_description.module_path,
            service_description.class_name,
            self.method_name
        )

        method_ref = rst.render_include_method(
            path=method_path,
            no_index=True,
            extras=[
                extra.render_extra() for extra in self.extras
            ]
        )

        return method_ref


class ExtraInstruction(namedtuple('ExtraInstruction', [
    'title', 'content'
])):
    def render_extra(self):
        return rst.render_instruction(self.title, self.content)


class SingleEvent(namedtuple('SingleEvent', [
    'event_path', 'extras'
])):
    def render_section(self, service_description, this_level=1):
        event_ref = rst.render_include_class(
            path=self.event_path,
            no_index=True,
            extras=[
                extra.render_extra() for extra in self.extras
            ]
        )

        return event_ref
