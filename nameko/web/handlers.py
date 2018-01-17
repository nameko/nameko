from functools import partial
from logging import getLogger

import six
from eventlet.event import Event
from werkzeug.routing import Rule
from werkzeug.wrappers import Response

from nameko.exceptions import BadRequest, serialize
from nameko.extensions import Entrypoint
from nameko.web.server import WebServer


_log = getLogger(__name__)


class HttpRequestHandler(Entrypoint):
    server = WebServer()

    def __init__(self, method, url, **kwargs):
        self.method = method
        self.url = url
        super(HttpRequestHandler, self).__init__(**kwargs)

    def get_url_rule(self):
        return Rule(self.url, methods=self.method.split(','))

    def setup(self):
        self.server.register_provider(self)

    def stop(self):
        self.server.unregister_provider(self)
        super(HttpRequestHandler, self).stop()

    def get_entrypoint_parameters(self, request):
        args = (request,)
        kwargs = request.path_values
        return args, kwargs

    def handle_request(self, request):
        request.shallow = False
        try:
            context_data = self.server.context_data_from_headers(request)
            args, kwargs = self.get_entrypoint_parameters(request)

            self.check_signature(args, kwargs)
            event = Event()
            self.container.spawn_worker(
                self, args, kwargs, context_data=context_data,
                handle_result=partial(self.handle_result, event))
            result = event.wait()

            response = self.response_from_result(result)

        except Exception as exc:
            response = self.response_from_exception(exc)
        return response

    def handle_result(self, event, worker_ctx, result, exc_info):
        event.send(result, exc_info)
        return result, exc_info

    def response_from_result(self, result):
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

        if not isinstance(payload, six.string_types):
            raise TypeError(
                "Payload must be a string. Got `{!r}`".format(payload)
            )

        return Response(
            payload,
            status=status,
            headers=headers,
        )

    def response_from_exception(self, exc):
        if (
            isinstance(exc, self.expected_exceptions) or
            isinstance(exc, BadRequest)
        ):
            status_code = 400
        else:
            status_code = 500
        error_dict = serialize(exc)
        payload = u'Error: {exc_type}: {value}\n'.format(**error_dict)

        return Response(
            payload,
            status=status_code,
        )


http = HttpRequestHandler.decorator
