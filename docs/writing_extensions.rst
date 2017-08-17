.. _writing_extensions:

Writing Extensions
==================

Structure
---------

Extensions should subclass :class:`nameko.extensions.Extension`. This base class provides the basic structure for an extension, in particular the following methods which can be overridden to add functionality:

.. sidebar:: Binding

    An extension in a service class is merely a declaration. When a service is :ref:`hosted <running_services>` by a :ref:`service container <containers>` its extensions are "bound" to the container.

    The binding process is transparent to a developer writing new extensions. The only consideration should be that :meth:`~nameko.extensions.Extension.__init__` is called in :meth:`~nameko.extensions.Extensions.bind` as well as at service class declaration time, so you should avoid side-effects in this method and use :meth:`~nameko.extensions.Extensions.setup` instead.

.. automethod:: nameko.extensions.Extension.setup

.. automethod:: nameko.extensions.Extension.start

.. automethod:: nameko.extensions.Extension.stop


Writing Dependency Providers
----------------------------

Almost every Nameko application will need to define its own dependencies -- maybe to interface with a database for which there is no :ref:`community extension <community_extensions>` or to communicate with a :ref:`specific web service <travis>`.

Dependency providers should subclass :class:`nameko.extensions.DependencyProvider` and implement a :meth:`~nameko.extensions.DependencyProvider.get_dependency` method that returns the object to be injected into service workers.

The recommended pattern is to inject the minimum required interface for the dependency. This reduces the test surface and makes it easier to exercise service code under test.

Dependency providers may also hook into the *worker lifecycle*. The following three methods are called on all dependency providers for every worker:

.. automethod:: nameko.extensions.DependencyProvider.worker_setup

.. automethod:: nameko.extensions.DependencyProvider.worker_result

.. automethod:: nameko.extensions.DependencyProvider.worker_teardown

Concurrency and Thread-Safety
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The object returned by :meth:`~nameko.extensions.DependencyProvider.get_dependency` should be thread-safe, because it may be accessed by multiple concurrently running workers.

The *worker lifecycle* are called in the same thread that executes the service method. This means, for example, that you can define thread-local variables and access them from each method.


Example
^^^^^^^

A simple ``DependencyProvider`` that sends messages to an SQS queue.

.. literalinclude:: examples/sqs_send.py


Writing Entrypoints
-------------------

You can implement new Entrypoint extensions if you want to support new transports or mechanisms for initiating service code.

The minimum requirements for an Entrypoint are:

1. Inherit from :class:`nameko.extensions.Entrypoint`
2. Implement the :meth:`~nameko.extensions.Entrypoint.start()` method to start the entrypoint when the container does. If a background thread is required, it's recommended to use a thread managed by the service container (see :ref:`spawning_background_threads`)
3. Call :meth:`~nameko.containers.ServiceContainer.spawn_worker()` on the bound container when appropriate.

Example
^^^^^^^

A simple ``Entrypoint`` that receives messages from an SQS queue.

.. literalinclude:: examples/sqs_receive.py

Used in a service:

.. literalinclude:: examples/sqs_service.py

Expected Exceptions
^^^^^^^^^^^^^^^^^^^

The Entrypoint base class constructor will accept a list of classes that should be considered to be "expected" if they are raised by the decorated service method. This can used to differentiate *user errors* from more fundamental execution errors. For example:

.. literalinclude:: examples/expected_exceptions.py
    :pyobject: Service

The list of expected exceptions are saved to the Entrypoint instance so they can later be inspected, for example by other extensions that process exceptions, as in `nameko-sentry <https://github.com/mattbennett/nameko-sentry/blob/b254ba99df5856030dfcb1d13b14c1c8a41108b9/nameko_sentry.py#L159-L164>`_.


Sensitive Arguments
^^^^^^^^^^^^^^^^^^^

In the same way as *expected exceptions*, the Entrypoint constructor allows you to mark certain arguments or parts of arguments as sensitive. For example:

.. literalinclude:: examples/sensitive_arguments.py
    :pyobject: Service

This can to be used in combination with the utility function :func:`nameko.utils.get_redacted_args`, which will return an entrypoint's call args (similar to :func:`inspect.getcallargs`) but with sensitive elements redacted.

This is useful in Extensions that log or save information about entrypoint invocations, such as `nameko-tracer <https://github.com/Overseas-Student-Living/nameko-tracer>`_.

For entrypoints that accept sensitive information nested within an otherwise safe argument, you can specify partial redaction. For example:

.. code-block:: python

    # by dictionary key
    @entrypoint(sensitive_arguments="foo.a")
    def method(self, foo):
        pass

    >>> get_redacted_args(method, foo={'a': 1, 'b': 2})
    ... {'foo': {'a': '******', 'b': 2}}

    # list index
    @entrypoint(sensitive_arguments="foo.a[1]")
    def method(self, foo):
        pass

    >>> get_redacted_args(method, foo=[{'a': [1, 2, 3]}])
    ... {'foo': {'a': [1, '******', 3]}}

Slices and relative list indexes are not supported.

.. _spawning_background_threads:

Spawning Background Threads
---------------------------

Extensions needing to execute work in a thread may choose to delegate the management of that thread to the service container using :meth:`~nameko.containers.ServiceContainer.spawn_managed_thread()`.

.. literalinclude:: examples/sqs_receive.py
    :pyobject: SqsReceive.start

Delegating thread management to the container is advised because:

* Managed threads will always be terminated when the container is stopped or killed.
* Unhandled exceptions in managed threads are caught by the container and will cause it to terminate with an appropriate message, which can prevent hung processes.
