from weakref import WeakKeyDictionary
import inspect

class_dependencies = WeakKeyDictionary()


def depends(**kwargs):
    def decorator(cls):
        class_dependencies[cls] = kwargs
        return cls

    return decorator


def inject_dependencies(service, connection):
    #need to iterate over all class instances
    for cls in reversed(inspect.getmro(type(service))[:-1]):
        try:
            dependencies = class_dependencies[cls]
        except KeyError:
            continue

        for name, dep in dependencies.items():
            setattr(service, name, dep.get_dependency(connection))
