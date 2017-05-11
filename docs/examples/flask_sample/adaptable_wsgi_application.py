# adaptable_wsgi_application.py

from functools import partial

from eventlet.event import Event

from werkzeug.wrappers import Response

from nameko.extensions import Entrypoint
from nameko.exceptions import serialize, BadRequest
from nameko.web.server import HttpOnlyProtocol
from nameko.web.server import WebServer
from nameko.runners import ServiceRunner
from nameko.testing.utils import get_container

from eventlet import wsgi
import eventlet


class MyWebServer(WebServer):
    def start(self):
        if not self._starting:
            self._starting = True
            self._wsgi_app = self.handle_wsgi_call
            self._sock = eventlet.listen(self.bind_addr)
            self._serv = wsgi.Server(self._sock,
                                     self._sock.getsockname(),
                                     self._wsgi_app,
                                     protocol=HttpOnlyProtocol,
                                     debug=False)
            self._gt = self.container.spawn_managed_thread(self.run)

    def handle_wsgi_call(self, environ, start_response):
        return list(self._providers)[0].handle_wsgi_call(environ, start_response)


class MyHttpRequestHandler(Entrypoint):
    server = MyWebServer()

    def __init__(self):
        pass

    def setup(self):
        self.server.register_provider(self)

    def stop(self):
        self.server.unregister_provider(self)
        super(MyHttpRequestHandler, self).stop()

    def handle_wsgi_call(self, environ, start_response):
        try:
            event = Event()
            args = (environ, start_response)
            kwargs = {}
            self.container.spawn_worker(
                self, args, kwargs, context_data={},
                handle_result=partial(self.handle_result, event))
            response = event.wait()
        except Exception as exc:
            response = self.response_from_exception(exc)(environ, start_response)
        return response

    def handle_result(self, event, worker_ctx, result, exc_info):
        event.send(result, exc_info)
        return result, exc_info

    def response_from_exception(self, exc):
        if isinstance(exc, BadRequest):
            status_code = 400
        else:
            status_code = 500
        error_dict = serialize(exc)
        payload = u'Error: {exc_type}: {value}\n'.format(**error_dict)

        return Response(payload, status=status_code)

dec = MyHttpRequestHandler.decorator

# ########################### Main Flask App BEGIN ############################
try:
    from flask import Flask, render_template, request, abort, redirect, url_for, Response as FlaskResponse
except ImportError:
    raise RuntimeError("flask is not installed, which is required to test this example")

app = Flask('adaptable_wsgi_application')


@app.route('/')
def home():
    return 'where am i, why is it dark here?'


@app.route('/template')
def template():
    return render_template('template.html', name='eightnoteight')


@app.route('/static_file')
def static_file():
    return redirect(url_for('static', filename='style.css'))


@app.route('/upload', methods=['POST'])
def upload_file():
    f = request.files['the_file']
    f.save('/tmp/tmp.txt')
    return ""


@app.route('/privileged', methods=['GET'])
def forbidden():
    abort(403)


@app.route('/redirect_to_home')
def redirect_to_home():
    return redirect('/')

# ########################### Main Flask App END ##############################

# ########################### Standard Service Pattern START ############################


class Service(object):
    name = "advanced_http_service"

    @dec
    def method(self, *args, **kwargs):
        return app(*args, **kwargs)

# ########################### Standard Service Pattern END ##############################

# ########################### Service Runner BEGIN ############################
runner = ServiceRunner(config={})
runner.add_service(Service)

container = get_container(runner, Service)

# start both services
runner.start()

try:
    # run continuously
    runner.wait()
except KeyboardInterrupt:
    # stop both services
    runner.stop()

# ########################### Service Runner END ##############################
