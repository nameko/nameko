from __future__ import absolute_import

import uuid
import json

from collections import defaultdict

from eventlet.event import Event
from eventlet.queue import Queue

from nameko.exceptions import deserialize


def make_virtual_socket(host, port, path='/ws'):
    from websocket import WebSocketApp

    result_handlers = {}

    class Socket(object):

        def __init__(self):
            self._event_queues = defaultdict(Queue)

        def get_event_queue(self, event_type):
            return self._event_queues[event_type]

        def wait_for_event(self, event_type):
            return self.get_event_queue(event_type).get()

        def rpc(self, _method, **data):
            id = str(uuid.uuid4())
            event = Event()
            result_handlers[id] = event.send
            ws_app.send(json.dumps({
                'method': _method,
                'data': data,
                'correlation_id': id,
            }))

            rv = event.wait()
            if rv['success']:
                return rv['data']
            raise deserialize(rv['error'])

    sock = Socket()

    def on_message(ws, message):
        msg = json.loads(message)
        if msg['type'] == 'event':
            sock.get_event_queue(msg['event']).put((msg['event'], msg['data']))
        elif msg['type'] == 'result':
            result_id = msg['correlation_id']
            handler = result_handlers.pop(result_id, None)
            if handler is not None:
                handler(msg)

    ready_event = Event()

    def on_open(ws):
        ready_event.send(None)

    def on_error(ws, err):
        ready_event.send(err)

    ws_app = WebSocketApp(
        'ws://%s:%d%s' % (host, port, path),
        on_message=on_message,
        on_open=on_open,
        on_error=on_error,
    )

    def connect_socket():
        err = ready_event.wait()
        if err is not None:
            raise err
        return sock

    return ws_app, connect_socket
