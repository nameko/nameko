def service_class(cls):
    return cls


def service_method(fn):
    return fn


def misdirection(fn):
    return fn


class ParentService(object):
    # Not included directly...

    @service_method
    def include(self):
        # But this is included because the class is extended.
        pass


@service_class
class Service(ParentService):
    # This class is included

    def red_herring(self):
        # This isn't included, not a service method
        pass

    @misdirection
    @service_method
    @misdirection
    def misdirected(self):
        # This is included, is a service method
        pass


class RedHerring(object):
    # Not included, not a service class

    @service_method
    def exclude(self):
        # Not included, service method but not in appropriate class
        pass
