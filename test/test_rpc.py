# def test_exceptions(get_connection):
#     class Controller(object):
#         def test_method(self, context, **kwargs):
#             raise KeyError('foo')

#     srv = service.Service(
#         Controller, connection_factory=get_connection,
#         exchange='testrpc', topic='test', )
#     srv.start()
#     eventlet.sleep()

#     try:
#         test = TestProxy(get_connection, timeout=3).test
#         with pytest.raises(exceptions.RemoteError):
#             test.test_method()

#         with pytest.raises(exceptions.RemoteError):
#             test.test_method_does_not_exist()
#     finally:
#         srv.kill()
