import json

from requests import HTTPError, Session, ConnectionError
from six.moves.urllib.parse import quote  # pylint: disable=E0401
import six

__all__ = ['Client', 'HTTPError']


def _quote(value):
    return quote(value, '')


class Client(object):
    """Pyrabbit replacement using requests instead of httplib2 """

    def __init__(self, uri):
        self._base_url = '{}/api'.format(uri)
        self._session = Session()
        self._session.headers['content-type'] = 'application/json'
        self._verify_api_connection()

    def _build_url(self, args):
        args = map(_quote, args)
        return '{}/{}'.format(
            self._base_url,
            '/'.join(args),
        )

    def _request(self, method, *args, **kwargs):
        url = self._build_url(args)
        json_data = kwargs.pop('json', None)
        if json_data is not None:
            kwargs['data'] = json.dumps(json_data)

        try:
            result = self._session.request(method, url, **kwargs)
        except ConnectionError as exc:
            six.raise_from(Exception(
                'Connection error for the RabbitMQ management HTTP'
                ' API at {}, is it enabled?'.format(url)
            ), exc)

        result.raise_for_status()
        if result.content:
            return result.json()

    def _get(self, *args, **kwargs):
        return self._request('GET', *args, **kwargs)

    def _put(self, *args, **kwargs):
        return self._request('PUT', *args, **kwargs)

    def _delete(self, *args, **kwargs):
        return self._request('DELETE', *args, **kwargs)

    def _post(self, *args, **kwargs):
        return self._request('POST', *args, **kwargs)

    def _verify_api_connection(self):
        self._get('overview')

    def get_connections(self):
        return self._get('connections')

    def delete_connection(self, name):
        return self._delete('connections', name)

    def get_exchanges(self, vhost):
        return self._get('exchanges', vhost)

    def get_all_vhosts(self):
        return self._get('vhosts')

    def create_vhost(self, vhost):
        return self._put('vhosts', vhost)

    def delete_vhost(self, vhost):
        return self._delete('vhosts', vhost)

    def set_vhost_permissions(self, vhost, username, configure, read, write):
        permissions = {
            'configure': configure,
            'read': read,
            'write': write,
        }
        return self._put(
            'permissions', vhost, username,
            json=permissions)

    def get_queue(self, vhost, name):
        return self._get('queues', vhost, name)

    def create_queue(self, vhost, name, **properties):
        return self._put('queues', vhost, name, json=properties)

    def get_queues(self, vhost):
        return self._get('queues', vhost)

    def get_queue_bindings(self, vhost, name):
        return self._get('queues', vhost, name, 'bindings')

    def create_queue_binding(self, vhost, exchange, queue, routing_key):
        body = {
            'routing_key': routing_key,
        }
        return self._post(
            'bindings', vhost, 'e', exchange, 'q', queue, json=body
        )

    def publish(self, vhost, name, routing_key, payload, properties=None):
        body = {
            'routing_key': routing_key,
            'payload': payload,
            'properties': properties or {},
            'payload_encoding': 'string',
        }
        return self._post('exchanges', vhost, name, 'publish', json=body)

    def get_messages(self, vhost, name, count=1, requeue=False):
        body = {
            'count': count,
            'encoding': 'auto',
            'requeue': requeue,
        }
        return self._post('queues', vhost, name, 'get', json=body)
