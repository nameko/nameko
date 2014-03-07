from .util import service_class as foo_cls, service_method as bar_meth


@foo_cls(1)
class Service(object):
    @bar_meth
    def renamed(self):
        pass
