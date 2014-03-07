from .step_three import GrandparentService as Renamed
from .util import service_method


class ParentService(Renamed):
    # Not included directly...

    @service_method
    def include(self):
        # But this is included because the class is extended.
        pass
