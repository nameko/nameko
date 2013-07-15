from nameko.proxy import RPCProxy

_calls = {}
_routes = {}
_services_whitelist = {}
_fallback_to_call = True


def reset_state():
    global _routes, _services_whitelist, _calls, _fallback_to_call
    _calls = {}
    _routes = {}
    _services_whitelist = {}
    _fallback_to_call = True


class MockRPCProxy(RPCProxy):
    def __init__(self, *args, **kwargs):
        super(MockRPCProxy, self).__init__(*args, **kwargs)
        self.reset()

    @staticmethod
    def reset():
        reset_state()

    @staticmethod
    def add_routing(topic, method, func, priority=0):
        routes = _routes.setdefault(topic, {}).setdefault(method, [])
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

    # keep self.fallback_to_call for backwards compatibility
    @property
    def fallback_to_call(self):
        return _fallback_to_call

    @fallback_to_call.setter
    def fallback_to_call(self, value):
        global _fallback_to_call
        _fallback_to_call = value

    def __call__(self, context=None, **kwargs):
        topic, method = self._get_route(kwargs.copy())

        if context is None:
            context = self.context_factory()

        _calls.append((topic, method, kwargs))

        # check the routes registered on the mock object first
        routes = _routes.get(topic, {}).get(method)
        if routes:
            for _, r in routes:
                try:
                    return r(context, **kwargs)
                except (ValueError, AssertionError):
                    pass

        # no route was registered so fallback to a real call if it's allowed
        if _fallback_to_call:
            whitelist = _services_whitelist
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
