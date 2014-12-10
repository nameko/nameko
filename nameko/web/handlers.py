import sys
import json
from logging import getLogger
from functools import partial

from eventlet.event import Event

from werkzeug.wrappers import Response
from werkzeug.routing import Rule

from nameko import exceptions
from nameko.dependencies import (
    CONTAINER_SHARED, entrypoint, EntrypointProvider, DependencyFactory)
from nameko.web.server import server
from nameko.web.protocol import JsonProtocol
from nameko.web.exceptions import expose_exception


_log = getLogger(__name__)


class BadPayload(Exception):
    pass


class RequestHandler(EntrypointProvider):
    server = server(shared=CONTAINER_SHARED)

    def __init__(self, method, url, expected_exceptions=None,
                 protocol=None):
        self.method = method
        self.url = url
        if protocol is None:
            protocol = JsonProtocol()
        self.protocol = protocol
        self.expected_exceptions = expected_exceptions

    def get_url_rule(self):
        return Rule(self.url, methods=[self.method])

    def prepare(self):
        self.server.register_provider(self)

    def stop(self):
        self.server.unregister_provider(self)
        super(RequestHandler, self).stop()

    def context_data_from_headers(self, request):
        return {}

    def add_url_payload(self, payload, request):
        payload.update(request.path_values)

    def process_request_data(self, request, load_payload=True):
        context_data = self.context_data_from_headers(request)
        if load_payload:
            payload = self.protocol.load_payload(request)
            if payload is None:
                payload = {}
            elif not isinstance(payload, dict):
                raise BadPayload('Dictionary expected')
        else:
            payload = {}
        self.add_url_payload(payload, request)
        return context_data, payload

    def handle_request(self, request):
        request.shallow = False
        try:
            context_data, payload = self.process_request_data(request)
            result = self.handle_message(context_data, payload)
            if isinstance(result, Response):
                return result
            return self.protocol.response_from_result(result)
        except Exception as e:
            exc_info = sys.exc_info()
            _log.error('request handling failed', exc_info=exc_info)
            return self.protocol.response_from_exception(
                e, expected_exceptions=self.expected_exceptions)

    def handle_message(self, context_data, payload):
        self.check_signature((), payload)
        event = Event()
        self.container.spawn_worker(self, (), payload,
                                    context_data=context_data,
                                    handle_result=partial(
                                        self.handle_result, event))
        return event.wait()

    def expose_exception(self, exc):
        is_operational, data = self.protocol.expose_exception(exc)

    def handle_result(self, event, worker_ctx, result, exc_info):
        event.send(result)
        return result, exc_info


@entrypoint
def http(method, url, expected_exceptions=None):
    return DependencyFactory(RequestHandler, method, url,
                             expected_exceptions=expected_exceptions)
