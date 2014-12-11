from __future__ import absolute_import

import uuid
import json

from eventlet.event import Event

from nameko.exceptions import RemoteError


def make_virtual_socket(host, port, path='/ws'):
    from websocket import WebSocketApp

    result_handlers = {}

    class Socket(object):

        def __init__(self):
            self.events_received = []
            self._event_signal = Event()

        def pop_event(self, event_type):
            for idx, evt in enumerate(self.events_received):
                if evt[0] == event_type:
                    del self.events_received[idx]
                    return evt

        def wait_for_event(self, event_type):
            while 1:
                evt = self.pop_event(event_type)
                if evt is not None:
                    return evt
                self._event_signal.wait()

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
            raise RemoteError(rv['error']['type'], rv['error']['message'])

    sock = Socket()

    def on_message(ws, message):
        msg = json.loads(message)
        if msg['type'] == 'event':
            sock.events_received.append((msg['event'], msg['data']))
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
