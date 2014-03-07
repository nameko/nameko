def service_class(cls):
    return cls


def service_method(fn):
    return fn


def misdirection(fn):
    return fn


@service_class
class Service(object):
    # This class is included

    def red_herring(self):
        # This isn't included, not a service method
        pass

    @service_method
    def include(self):
        # This is included
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
