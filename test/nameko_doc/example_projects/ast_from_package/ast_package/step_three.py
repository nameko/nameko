from .util import service_method


class GrandparentService(object):
    # Not included directly...

    @service_method
    def grandparent_include(self):
        # But this is included because the class is extended.
        pass
