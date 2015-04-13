"""
Nameko components that can be used as standalone tools, without being hosted
inside a nameko-managed service.

Intended to be used as test utilities and external controls, for example to
initiate some action inside a nameko cluster.

.. Example:

    Use the RPC proxy to perform some addition on "mathsservice"::

        >>> from nameko.standalone.rpc import rpc_proxy
        >>>
        >>> with rpc_proxy("mathsservice", config) as proxy:
        ...     result = proxy.add(2, 2)
        ...
        >>> print result
        4

.. Example:

    Dispatch a ``custom_event`` as ``srcservice``::

    >>> from nameko.standalone.events import event_dispatcher
    >>>
    >>> with event_dispatcher("srcservice", config) as dispatch:
    ...     dispatch("custom_event", "msg")
    ...
    >>>

"""
