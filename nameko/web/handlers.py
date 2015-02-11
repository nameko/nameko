import sys
from logging import getLogger
from functools import partial

from eventlet.event import Event

from werkzeug.wrappers import Response
from werkzeug.routing import Rule

from nameko.extensions import Entrypoint
from nameko.web.server import WebServer
from nameko.web.protocol import PlaintextProtocol


_log = getLogger(__name__)


class HttpRequestHandler(Entrypoint):
    server = WebServer()

    def __init__(self, method, url, expected_exceptions=(), protocol=None):
        self.method = method
        self.url = url
        if protocol is None:
            protocol = PlaintextProtocol()
        self.protocol = protocol
        self.expected_exceptions = expected_exceptions

    def get_url_rule(self):
        return Rule(self.url, methods=[self.method])

    def setup(self):
        self.server.register_provider(self)

    def stop(self):
        self.server.unregister_provider(self)
        super(HttpRequestHandler, self).stop()

    def process_request_data(self, request):
        context_data = self.server.context_data_from_headers(request)
        args, kwargs = self.protocol.load_payload(request)
        return context_data, args, kwargs

    def add_url_kwargs(self, kwargs, request):
        kwargs.update(request.path_values)

    def handle_request(self, request):
        request.shallow = False
        try:
            context_data, args, kwargs = self.process_request_data(request)
            self.add_url_kwargs(kwargs, request)
            result = self.handle_message(context_data, args, kwargs)
            if isinstance(result, Response):
                return result
            return self.protocol.response_from_result(result)
        except Exception as exc:
            exc_info = sys.exc_info()
            _log.error('request handling failed', exc_info=exc_info)
            return self.protocol.response_from_exception(
                exc, expected_exceptions=self.expected_exceptions)

    def handle_message(self, context_data, args, kwargs):
        self.check_signature(args, kwargs)
        event = Event()
        self.container.spawn_worker(
            self, args, kwargs, context_data=context_data,
            handle_result=partial(self.handle_result, event))
        return event.wait()

    def handle_result(self, event, worker_ctx, result, exc_info):
        event.send(result, exc_info)
        return result, exc_info


http = HttpRequestHandler.decorator
