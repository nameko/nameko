import sys
import json
from logging import getLogger
from functools import partial

from eventlet.event import Event

from werkzeug.wrappers import Response
from werkzeug.routing import Rule

from nameko import exceptions
from nameko.web.server import server
from nameko.web.exceptions import expose_exception
from nameko.dependencies import (
    CONTAINER_SHARED, entrypoint, EntrypointProvider, DependencyFactory)


_log = getLogger(__name__)


class BadPayload(Exception):
    pass


class RequestHandler(EntrypointProvider):
    server = server(shared=CONTAINER_SHARED)

    def __init__(self, method, url, expected_exceptions=None):
        self.method = method
        self.url = url
        self.expected_exceptions = expected_exceptions

    def get_url_rule(self):
        return Rule(self.url, methods=[self.method],
                    endpoint='%s.%s' % (
                        self.container.service_name, self.name))

    def prepare(self):
        self.server.register_provider(self)

    def stop(self):
        self.server.unregister_provider(self)
        super(RequestHandler, self).stop()

    def context_data_from_headers(self, request):
        return {}

    def load_payload(self, request):
        return None

    def add_url_payload(self, payload, request):
        payload.update(request.path_values)

    def process_request_data(self, request):
        context_data = self.context_data_from_headers(request)
        payload = self.load_payload(request)
        if payload is None:
            payload = {}
        elif not isinstance(payload, dict):
            raise BadPayload('Dictionary expected')
        self.add_url_payload(payload, request)
        return context_data, payload

    def describe_response(self, request, result):
        headers = None
        if isinstance(result, tuple):
            if len(result) == 3:
                status, headers, payload = result
            else:
                status, payload = result
        else:
            payload = result
            status = 200
        return status, headers, payload

    def response_from_result(self, request, result):
        raise NotImplementedError()

    def response_from_exception(self, request, exc_type, exc_value, tb):
        raise NotImplementedError()

    def handle_request(self, request):
        try:
            context_data, payload = self.process_request_data(request)
            result = self.handle_message(context_data, payload)
            if isinstance(result, Response):
                return result
            return self.response_from_result(request, result)
        except Exception:
            exc_info = sys.exc_info()
            _log.error('request handling failed', exc_info=exc_info)
            return self.response_from_exception(request, *exc_info)

    def payload_to_args(self, payload):
        return (), payload

    def handle_message(self, context_data, payload):
        args, kwargs = self.payload_to_args(payload)
        self.check_signature(args, kwargs)
        event = Event()
        self.container.spawn_worker(self, (), payload,
                                    context_data=context_data,
                                    handle_result=partial(
                                        self.handle_result, event))
        return event.wait()

    def expose_exception(self, exc):
        is_operational, data = expose_exception(exc)
        if is_operational or (self.expected_exceptions and
                              isinstance(exc, self.expected_exceptions)):
            status_code = 400
        else:
            status_code = 500
        return status_code, data

    def handle_result(self, event, worker_ctx, result, exc_info):
        event.send(result)
        return result, exc_info


class JsonRequestHandler(RequestHandler):

    def load_payload(self, request):
        if request.mimetype == 'application/json':
            try:
                return json.load(request.stream)
            except Exception as e:
                raise BadPayload('Invalid JSON data')

    def _serialize_payload(self, request, payload, success=True):
        if success:
            wrapper = {'success': True, 'data': payload}
        else:
            wrapper = {'success': False, 'error': payload}
        return json.dumps(wrapper)

    def response_from_result(self, request, result):
        status, headers, payload = self.describe_response(request, result)
        return Response(self._serialize_payload(request, payload, True),
                        status=status, headers=headers,
                        mimetype='application/json')

    def response_from_exception(self, request, exc_type, exc_value, tb):
        status_code, payload = self.expose_exception(exc_value)
        return Response(self._serialize_payload(
            request, payload, False),
            status=status_code, mimetype='application/json')


@entrypoint
def http(method, url, expected_exceptions=None):
    return DependencyFactory(JsonRequestHandler, method, url,
                             expected_exceptions=expected_exceptions)
