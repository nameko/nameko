import rst_render as rst


class ServiceCollection(object):
    def __init__(self, services=None):
        self.services = services or []

    def __eq__(self, other):
        return self.services == other.services

    def render(self, rst_page_printer):
        """ Render this service collection out """
        for service in self.services:
            page = service.render_page()
            rst_page_printer.add_page(page)


class ServiceDescription(object):
    def __init__(self, name, module_path, class_name, sections=None):
        self.name = name
        self.module_path = module_path
        self.class_name = class_name
        self.sections = sections or []

    def __eq__(self, other):
        return (
            self.name == other.name and
            self.module_path == other.module_path and
            self.class_name == other.class_name and
            self.sections == other.sections
        )

    def render_page(self):
        """ Render a service into a page """
        section_parts = [
            section.render_section(self, 2) for section in self.sections
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


class ReferenceSection(object):
    def __init__(self, references=None):
        self.references = references or []

    def __eq__(self, other):
        return self.references == other.references

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


class ClassReference(object):
    def __init__(self, title, reference_path):
        self.title = title
        self.reference_path = reference_path

    def __eq__(self, other):
        return (
            self.title == other.title and
            self.reference_path == other.reference_path
        )

    def render_reference(self):
        """ Render a reference to a class under a given category/title """
        return rst.render_definition(
            term=self.title,
            description=rst.render_class_reference(
                path=self.reference_path
            )
        )


class Section(object):
    def __init__(self, title, contents=None):
        self.title = title
        self.contents = contents or []

    def __eq__(self, other):
        return (
            self.title == other.title and
            self.contents == other.contents
        )

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


class SingleMethod(object):
    def __init__(self, method_name, extras=None):
        self.method_name = method_name
        self.extras = extras or []

    def __eq__(self, other):
        return (
            self.method_name == other.method_name and
            self.extras == other.extras
        )

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


class ExtraInstruction(object):
    def __init__(self, title, content):
        self.title = title
        self.content = content

    def render_extra(self):
        return rst.render_instruction(self.title, self.content)
