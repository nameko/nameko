from nameko.proxy import RPCProxy


class MockRPCProxy(RPCProxy):
    def __init__(self, fallback_to_call=True, *args, **kwargs):
        super(MockRPCProxy, self).__init__(*args, **kwargs)
        self.fallback_to_call = fallback_to_call
        self.reset(keep_routings=False)

    def reset(self, keep_routings=False):
        if not keep_routings:
            self._routings = {}
        self._calls = []
        self._services_whitelist = []

    def __getattr__(self, key):
        attr = super(MockRPCProxy, self).__getattr__(key)
        attr._routings = self._routings
        attr._calls = self._calls
        attr._services_whitelist = self._services_whitelist
        return attr

    def add_routing(self, topic, method, func, priority=0):
        routings = self._routings.setdefault(topic, {}).setdefault(method, [])
        routings.append((priority, func))
        routings.sort(reverse=True)

    def add_dummy(self, topic, method, retvalue=None, priority=0):
        self.add_routing(
            topic, method, lambda *args, **kwargs: retvalue,
            priority=priority)

    def add_call_matching(self, topic, method, args, returnvalue):
        def routefunc(context, **kwargs):
            assert kwargs == args
            return returnvalue
        self.add_routing(topic, method, routefunc)

    def __call__(self, context=None, **kwargs):
        topic, method = self._get_route(kwargs.copy())

        if context is None:
            context = self.context_factory()

        self._calls.append((topic, method, kwargs))

        # check the routes registered on the mock object first
        routings = self._routings.get(topic, {}).get(method)
        if routings:
            for pri, r in routings:
                try:
                    return r(context, **kwargs)
                except (ValueError, AssertionError):
                    pass

        # no route was registered so fallback to a real call if it's allowed
        if self.fallback_to_call:
            whitelist = self._services_whitelist
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
