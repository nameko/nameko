from contextlib import contextmanager

from mock import patch, Mock


@contextmanager
def patch_injection_provider(provider):
    """ Patches an `InjectionProvider` provider's acquire_injection
    such that it returns a `Mock` as the injection object.
    The injection object will be yielded by the contextmanager.

    example:

        class MyService(object):
            dispatch=EventDispatcher()

            @rpc
            def foo(self):
                self.dispatch(MyEvent())


        def test_service_dispatches_event():

            test_srv = TestProxy(MyService)

            with patch_attr_dependency(MyService.dispatch) as dispatch:
                test_srv.foo()
                dispatch.assert_called_once_with(Any)

    """
    injection = Mock()

    with patch.object(provider, 'acquire_injection') as acquire:
        acquire.return_value = injection
        yield injection
