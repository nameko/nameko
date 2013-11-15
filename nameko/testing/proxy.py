from nameko.legacy.proxy import RPCProxy

_state = {}


def reset_state():
    _state['calls'] = []
    _state['routes'] = {}
    _state['services_whitelist'] = []
    _state['fallback_to_call'] = True

reset_state()


class MockRPCProxy(RPCProxy):
    def __init__(self, *args, **kwargs):
        super(MockRPCProxy, self).__init__(*args, **kwargs)

    @staticmethod
    def reset():
        reset_state()

    @staticmethod
    def add_routing(topic, method, func, priority=0):
        routes = _state['routes'].setdefault(topic, {}).setdefault(method, [])
        routes.append((priority, func))
        routes.sort(reverse=True)

    @staticmethod
    def add_dummy(topic, method, retvalue=None, priority=0):
        MockRPCProxy.add_routing(
            topic, method, lambda *args, **kwargs: retvalue,
            priority=priority)

    @staticmethod
    def add_call_matching(topic, method, args, returnvalue):
        def routefunc(context, **kwargs):
            assert kwargs == args
            return returnvalue
        MockRPCProxy.add_routing(topic, method, routefunc)

    @staticmethod
    def add_service_to_whitelist(service):
        _state['fallback_to_call'] = True
        _state['services_whitelist'].append(service)

    # keep self.fallback_to_call for backwards compatibility
    @property
    def fallback_to_call(self):
        return _state['fallback_to_call']

    @fallback_to_call.setter
    def fallback_to_call(self, value):
        _state['fallback_to_call'] = value

    # keep self._calls for backwards compatibility
    @property
    def _calls(self):
        return _state['calls']

    def __call__(self, context=None, **kwargs):
        topic, method = self._get_route(kwargs.copy())

        if context is None:
            context = self.context_factory()

        _state['calls'].append((topic, method, kwargs))

        # check the routes registered on the mock object first
        routes = _state['routes'].get(topic, {}).get(method)
        if routes:
            for _, r in routes:
                try:
                    return r(context, **kwargs)
                except (ValueError, AssertionError):
                    pass

        # no route was registered so fallback to a real call if it's allowed
        if _state['fallback_to_call']:
            whitelist = _state['services_whitelist']
            if whitelist and topic not in whitelist:
                raise RuntimeError(
                    'Service not in whitelist when trying '
                    'to call {}.{}(args={})'.format(topic, method, kwargs)
                )
            return self.call(context, **kwargs)

        # unable to process the requested route
        raise RuntimeError(
            'No Matching RPC Routes for {}.{}(args={})'.format(
                topic, method, kwargs)
        )
