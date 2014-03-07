import ast
import imp
import inspect
import logging
import pkgutil
import textwrap

from nameko.rpc import RpcProvider


log = logging.getLogger(__name__)


def get_parsed_root_for_package(containing_folder, package_name):
    module = _get_package_root_module_from_containing_folder(containing_folder,
                                                             package_name)
    tree = _tree_from_package_root_module(module)
    return module, tree


def get_parsed_root_for_loaded_package(fq_name):
    module = __import__(fq_name)
    pieces = fq_name.split('.')[1:]
    for piece in pieces:
        module = getattr(module, piece)

    tree = _tree_from_package_root_module(module)
    return module, tree


def get_parsed_module_from_file(source_path, module_name):
    source = source_path.text()
    module = _get_module_from_containing_folder(source_path, module_name)
    tree = ast.parse(source)
    return module, tree


def _ast_visitor_factory(**handlers):
    fn_dict = {
        'visit_{}'.format(handler_type): handler_fn
        for handler_type, handler_fn in handlers.items()
    }
    visitor = type('Visitor', (ast.NodeVisitor,), fn_dict)
    return visitor


def _get_package_root_module_from_containing_folder(source_path, package_name):
    # package_name is a package directly inside source_path
    importer = pkgutil.get_importer(source_path)
    loader = importer.find_module(package_name)
    module = loader.load_module(package_name)
    return module


def _get_module_from_containing_folder(source_path, module_name):
    # module_name is a module directly inside source_path
    module = imp.load_source(module_name, source_path)
    return module


def _tree_from_package_root_module(module):
    try:
        source = inspect.getsource(module)
    except IOError:
        # If the module is empty (e.g. __init__.py), this happen
        tree = ast.parse('')
    else:
        tree = ast.parse(source)
    return tree


def _get_decorator_call_name(decorator, node):
    if hasattr(decorator, 'id'):
        return decorator.id
    elif hasattr(decorator, 'attr'):
        return decorator.attr
    elif hasattr(decorator, 'func'):
        return _get_decorator_call_name(decorator.func, node)


class AstDecoratorBasedInterpreter(object):
    def __init__(self, service_class_decorators, service_method_decorators):
        self.service_class_decorators = service_class_decorators
        self.service_method_decorators = service_method_decorators

    def process_services(self, module, module_tree):
        res = {}
        service_classes = self.service_classes_from_module_tree(module_tree)

        for service_cls in service_classes:
            service_cls_tree = service_cls['node']
            service_cls_obj = getattr(module, service_cls_tree.name)
            service_methods = self.service_methods_from_class_tree(
                module_tree, service_cls_obj)
            res[service_cls_tree.name] = {
                'class': service_cls_tree,
                'methods': service_methods
            }

        return res

    def service_classes_from_module_tree(self, tree):
        base_names_for_service_decorators = self.service_class_decorators
        decorators_for_service_classes = self._get_import_names(
            tree, base_names_for_service_decorators)

        service_classes = self.lookup_by_decorator(
            tree, decorators_for_service_classes, is_class=True)
        return service_classes

    def service_methods_from_class_tree(self, module_tree, class_obj):
        base_names_for_service_methods = self.service_method_decorators
        decorators_for_service_methods = self._get_import_names(
            module_tree, base_names_for_service_methods)

        service_methods = []

        cls_method_data = inspect.getmembers(class_obj, inspect.ismethod)
        for method_name, method in cls_method_data:
            method_source_code = inspect.getsource(method)
            method_tree = ast.parse(textwrap.dedent(method_source_code))

            match_data = self.method_is_for_service(
                method_name, method, method_tree,
                decorators_for_service_methods,
            )
            if match_data:
                service_methods.append(match_data)

        return service_methods

    def method_is_for_service(self, method_name, method, method_tree,
                              decorators_for_service_methods):
        # First check for Nameko style entrypoints...
        if hasattr(method, 'nameko_entrypoints'):
            entrypoint_dep_classes = {
                factory.dep_cls for factory in method.nameko_entrypoints
            }
            if any(issubclass(dep_cls, RpcProvider)
                   for dep_cls in entrypoint_dep_classes):
                # It's an RPC method
                match_data = {
                    'node': method_tree,
                    'trigger_name': 'RpcProvider',
                    'assigned_name': method_name,
                }
                return match_data

        # ...And then fall back to decorator name matching
        method_matches = self.lookup_by_decorator(
            method_tree, decorators_for_service_methods, is_class=False
        )
        if method_matches:
            # We're searching for functions and providing a function, so
            # we can take the first result.
            assert len(method_matches) == 1
            match_data = method_matches[0]
            match_data['assigned_name'] = method_name
            return match_data

        return None

    def lookup_by_decorator(self, tree, matching_names, is_class=False):
        res = []

        def handler(_, node):
            decorator_names = []
            for decorator in node.decorator_list:
                call_name = _get_decorator_call_name(decorator, node)
                if call_name in matching_names:
                    decorator_names.append(matching_names[call_name])

            if decorator_names:
                res.append({
                    'node': node,
                    # The first decorator is considered to be the one matching.
                    'trigger_name': decorator_names[0],
                })

        handlers = {
            'ClassDef' if is_class else 'FunctionDef': handler
        }
        searcher_cls = _ast_visitor_factory(**handlers)
        searcher_cls().visit(tree)
        return res

    def _get_import_names(self, tree, base_names):
        imported_names = {
            base_name: base_name
            for base_name in base_names
        }

        def handler(_, node):
            for alias in node.names:
                if alias.name in base_names:
                    imported_names[alias.asname or alias.name] = alias.name

        visitor_cls = _ast_visitor_factory(ImportFrom=handler)
        visitor_cls().visit(tree)
        return imported_names
