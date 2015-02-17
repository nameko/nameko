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


Dependency Providers
--------------------

It's likely that even a modest Nameko application will need to define its own dependencies -- maybe to interface with a database for which there is no :ref:`community extension <community_extensions>` or to communicate with a :ref:`specific web service <travis>`.

Dependency providers should subclass :class:`nameko.extensions.DependencyProvider` and implement a :meth:`~nameko.extensions.DependencyProvider.get_dependency` method that returns an object to be injected into service workers.

Dependency providers may also hook into the *worker lifecycle*. The following three methods are called on all dependency providers for every worker:

.. automethod:: nameko.extensions.DependencyProvider.worker_setup

.. automethod:: nameko.extensions.DependencyProvider.worker_result

.. automethod:: nameko.extensions.DependencyProvider.worker_teardown
