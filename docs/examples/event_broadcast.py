from nameko.events import BROADCAST, event_handler

class ListenerService(object):
    name = "listener"

    @event_handler("monitor", "ping", handler_type=BROADCAST)
    def ping(self, payload):
        # all running services will respond
        print("pong from {}".format(self.name))
