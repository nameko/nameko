import logging

from nameko.rpc import rpc


log = logging.getLogger('test.sample')


class Service(object):
    name = "service"

    @rpc
    def ping(self):
        log.info('ping!')
