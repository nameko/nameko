import sys
import json
from logging import getLogger
from functools import partial

from eventlet.event import Event

from werkzeug.wrappers import Response
from werkzeug.routing import Rule

from nameko.web.server import server
from nameko.dependencies import (
    CONTAINER_SHARED, entrypoint, EntrypointProvider, DependencyFactory)


_log = getLogger(__name__)


class BadPayload(Exception):
    pass


class RequestHandler(EntrypointProvider):
    server = server(shared=CONTAINER_SHARED)

    def __init__(self, method, url):
        self.method = method
        self.url = url

    def iter_url_rules(self):
        yield Rule(self.url, methods=[self.method],
                   endpoint='%s.%s' % (
                       self.container.service_name, self.name))

    def prepare(self):
        self.server.register_provider(self)

    def stop(self):
        self.server.unregister_provider(self)
        super(RequestHandler, self).stop()

    def context_data_from_headers(self, request):
        return {}

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

    def response_from_result(self, request, result):
        raise NotImplementedError()

    def response_from_exception(self, request, exc_type, exc_value, tb):
        raise NotImplementedError()

    def handle_request(self, request):
        try:
            context_data, payload = self.process_request_data(request)
            result = self.handle_message(context_data, payload)
            return self.response_from_result(request, result)
        except Exception:
            return self.response_from_exception(request, *sys.exc_info())

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

    def serialize_payload(self, request, payload, success=True):
        if success:
            wrapper = {'success': True, 'data': payload}
        else:
            wrapper = {'success': False, 'error': payload}
        return json.dumps(wrapper)

    def response_from_result(self, request, result):
        if isinstance(result, Response):
            return result

        headers = None
        if isinstance(result, tuple):
            if len(result) == 3:
                status, headers, payload = result
            else:
                status, payload = result
        else:
            payload = result
            status = 200
        return Response(self.serialize_payload(request, payload, True),
                        status=status, headers=headers,
                        mimetype='application/json')

    def response_from_exception(self, request, exc_type, exc_value, tb):
        _log.error('request handling failed', exc_info=(exc_type, exc_value, tb))
        return Response(self.serialize_payload(request, str(exc_value), False),
                        status=400, mimetype='application/json')


@entrypoint
def http(method, url):
    return DependencyFactory(JsonRequestHandler, method, url)
