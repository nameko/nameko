from nameko.events import event_handler, SERVICE_POOL, BROADCAST, SINGLETON


class ServiceBase(object):
    """ Base class with an event handler of each type.
    """
    name = None

    @event_handler('src_service', 'service_pool', handler_type=SERVICE_POOL)
    def handle_one(self, payload):
        print "{} handling 'service_pool' event"

    @event_handler('src_service', 'broadcast', handler_type=BROADCAST)
    def handle_two(self, payload):
        print "{} handling 'broadcast' event"

    @event_handler('src_service', 'singleton', handler_type=SINGLETON)
    def handle_three(self, payload):
        print "{} handling 'singleton' event"


class ServiceA(ServiceBase):
    """ Extend ``ServiceBase`` as "service_a"
    """
    name = "service_a"


class ServiceB(ServiceBase):
    """ Extend ``ServiceBase`` as "service_b"
    """
    name = "service_b"
