import logging

from nameko.rpc import rpc


log = logging.getLogger('test.anonymous')


class Service1(object):
    '''Anonymouse cls'''


class Service2(Service1):
    name = 'service2'

    @rpc
    def ping(self):
        log.info('ping')
