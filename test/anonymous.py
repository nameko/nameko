import logging

from nameko.rpc import rpc


log = logging.getLogger('test.anonymous')


class Service1(object):
    '''Anonymouse cls'''

    @rpc
    def ping1(self):
        log.info('ping1!')


class Service2(Service1):
    name = 'service2'

    @rpc
    def ping2(self):
        log.info('ping2!')
