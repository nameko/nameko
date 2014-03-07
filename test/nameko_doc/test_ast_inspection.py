import pytest
from nameko.nameko_doc.extractors.ast_inspection import (
    get_parsed_root_for_package, AstDecoratorBasedInterpreter,
    get_parsed_module_from_file)


@pytest.fixture(scope='session')
def interpreter():
    return AstDecoratorBasedInterpreter(
        service_class_decorators={'service_class'},
        service_method_decorators={'service_method'},
    )


def test_no_inheritance(copy_example, interpreter):
    source_folder = copy_example('ast')
    filename = 'no_inheritance.py'
    source_path = source_folder.joinpath(filename)

    module, tree = get_parsed_module_from_file(source_path, 'no_inheritance')

    processed = interpreter.process_services(module, tree)
    assert processed.keys() == ['Service']
    service_methods = processed['Service']['methods']
    assert sorted(m['node'].name for m in service_methods) == [
        'include', 'misdirected']


def test_inheritance_single_file(copy_example, interpreter):
    # There's always a little inheritance -- classes come from object.
    source_folder = copy_example('ast')
    filename = 'inheritance_single.py'
    source_path = source_folder.joinpath(filename)

    module, tree = get_parsed_module_from_file(source_path,
                                               'inheritance_single')

    processed = interpreter.process_services(module, tree)
    assert processed.keys() == ['Service']
    service_methods = processed['Service']['methods']
    assert sorted(m['node'].name for m in service_methods) == [
        'include', 'misdirected']


def test_inheritance_multiple_files(copy_example, interpreter):
    source_folder = copy_example('ast_from_package')
    module, tree = get_parsed_root_for_package(source_folder, 'ast_package')

    processed = interpreter.process_services(module, tree)
    assert processed.keys() == ['Service']
    service_methods = processed['Service']['methods']
    assert sorted(m['node'].name for m in service_methods) == [
        'grandparent_include', 'include']


def test_renamed_decorator(copy_example, interpreter):
    source_folder = copy_example('ast_from_package')
    package_name = 'ast_renaming'

    module, tree = get_parsed_root_for_package(source_folder, package_name)

    processed = interpreter.process_services(module, tree)
    assert processed.keys() == ['Service']
    service_methods = processed['Service']['methods']
    assert sorted(m['node'].name for m in service_methods) == ['renamed']
    assert [m['trigger_name'] for m in service_methods] == ['service_method']
