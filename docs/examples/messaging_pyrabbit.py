import eventlet
import urlparse
import uuid

from pyrabbit.api import Client

rabbit_ctl_uri = urlparse.urlparse('http://guest:guest@localhost:15672')
host_port = '{0.hostname}:{0.port}'.format(rabbit_ctl_uri)
client = Client(host_port, rabbit_ctl_uri.username, rabbit_ctl_uri.password)


def generate_msgs():
    while True:
        payload = "log-{}".format(uuid.uuid4())
        client.publish('/', 'demo_ex', '', payload)
        eventlet.sleep(3)


def main():
    try:
        generate_msgs()
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    main()
