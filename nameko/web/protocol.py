import json

from werkzeug.wrappers import Response

from nameko.exceptions import serialize, BadRequest, MalformedRequest


class BaseProtocol(object):
    # werkzeug will default to text/plain unless there is a content-type header
    mimetype = None

    def load_payload(self, request):
        return (), {}

    def response_from_result(self, result):
        headers = None
        if isinstance(result, tuple):
            if len(result) == 3:
                status, headers, payload = result
            else:
                status, payload = result
        else:
            payload = result
            status = 200

        return Response(
            self.serialize_result(payload, True),
            status=status,
            headers=headers,
            mimetype=self.mimetype,
        )

    def response_from_exception(self, exc, expected_exceptions=()):
        if (
            isinstance(exc, expected_exceptions) or
            isinstance(exc, BadRequest)
        ):
            status_code = 400
        else:
            status_code = 500
        payload = serialize(exc)
        return Response(
            self.serialize_result(payload, success=False),
            status=status_code,
        )


class PlaintextProtocol(BaseProtocol):
    def load_payload(self, request):
        if request.method == 'POST':
            data = request.get_data()
            return (data,), {}
        else:
            return super(PlaintextProtocol, self).load_payload(request)

    def serialize_result(
        self, payload, success=True, correlation_id=None,
    ):
        if not success:
            # this is a serialized exception
            return 'Error: {exc_type}: {value}\n'.format(**payload)
        return unicode(payload)


class JsonProtocol(BaseProtocol):
    mimetype = 'application/json'

    def deserialize_ws_frame(self, payload):
        try:
            data = json.loads(payload)
            return (
                data['method'],
                data.get('data') or {},
                data.get('correlation_id'),
            )
        except Exception:
            raise MalformedRequest('Invalid JSON data')

    def serialize_result(
        self, payload, success=True, ws=False, correlation_id=None,
    ):
        if success:
            wrapper = {'success': True, 'data': payload}
        else:
            wrapper = {'success': False, 'error': payload}
        if ws:
            wrapper['type'] = 'result'
        if ws or correlation_id is not None:
            wrapper['correlation_id'] = correlation_id
        return unicode(json.dumps(wrapper))

    def serialize_event(self, event, data):
        return unicode(json.dumps({
            'type': 'event',
            'event': event,
            'data': data,
        }))
