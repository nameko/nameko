from nameko.rpc import rpc, RpcProxy
from nameko.standalone.rpc import ServiceRpcProxy
from nameko.testing.utils import get_container
from nameko.testing.services import entrypoint_hook
import pytest

from nameko.exceptions import remote_error


class RemoteError(Exception):
    pass


@remote_error('test.test_remote_error.RemoteError')
class LocalError(Exception):
    pass


class RemoteService:

    name = 'remote-service'

    @rpc
    def raise_(self):
        raise RemoteError('Ya!')


class LocalService:

    name = 'local-service'

    remote = RpcProxy('remote-service')

    @rpc
    def catch(self):
        try:
            self.remote.raise_()
        except LocalError as exc:
            return 'Got {}'.format(exc.args[0])


def test_for_rpc_proxy_standalone(container_factory, rabbit_config):

    container = container_factory(RemoteService, rabbit_config)
    container.start()

    with ServiceRpcProxy('remote-service', rabbit_config) as proxy:
        with pytest.raises(LocalError) as exc:
            proxy.raise_()
        assert 'LocalError' in str(exc)
        assert 'Ya!' == str(exc.value)


def test_for_rpc_proxy_dependency_provider(runner_factory, rabbit_config):

    runner = runner_factory(rabbit_config, RemoteService, LocalService)
    runner.start()

    container = get_container(runner, LocalService)
    with entrypoint_hook(container, 'catch') as entrypoint:
        assert entrypoint() == "Got Ya!"
