Nameko Doc
==========

``nameko_doc`` is a tool designed to help produce documentation source files for
`Nameko <https://github.com/onefinestay/nameko>`_ based services, with a focus
on caller-oriented documentation. The aim is to help produce documentation
tightly aimed at a developer calling into a service cluster, specifically
including service RPC methods and leaving out things like unexposed public
methods.

Usage
-----

.. code-block:: bash

    nameko_doc -s <service_listing_function> -o <output_dir>

The above command will call the function specified by the dotted path in
``<service_listing_function>``, producing output as Sphinx source .rst files
in ``<output_dir>``. Or, use ``nameko_doc --help`` for further options.

How does this compare to ``sphinx-apidoc``?
-------------------------------------------

``sphinx-apidoc`` will attempt to produce ``.rst`` files for an entire module
path; ``nameko_doc`` uses a call to produce ``.rst`` files for just
service related content. At onefinestay, we use the two together -- standard
API docs are consumed by the service engineers, and ``nameko_doc`` docs are used
by front-end developers.

Identifying Services
--------------------

The function passed in to ``nameko_doc`` should return a list of tuples, where
each pair gives the service name and the corresponding service class.

From there, ``nameko_doc`` will check for any methods in each service class
that have been marked with nameko's built in ``@rpc`` or something that
inherits from it.

nameko.nameko_doc package
=========================

Submodules
----------

nameko.nameko_doc.entities module
---------------------------------

.. automodule:: nameko.nameko_doc.entities
    :members:
    :undoc-members:
    :show-inheritance:


nameko.nameko_doc.main module
-----------------------------

.. automodule:: nameko.nameko_doc.main
    :members:
    :undoc-members:
    :show-inheritance:


nameko.nameko_doc.method_extractor module
-----------------------------------------

.. automodule:: nameko.nameko_doc.method_extractor
    :members:
    :undoc-members:
    :show-inheritance:


nameko.nameko_doc.processor module
----------------------------------

.. automodule:: nameko.nameko_doc.processor
    :members:
    :undoc-members:
    :show-inheritance:


nameko.nameko_doc.rst_render module
-----------------------------------

.. automodule:: nameko.nameko_doc.rst_render
    :members:
    :undoc-members:
    :show-inheritance:


Module contents
---------------

.. automodule:: nameko.nameko_doc
    :members:
    :undoc-members:
    :show-inheritance:


Example/Target Output
=====================

.. code-block:: rst

    ``example_service``
    ===================

    .. automodule:: nameko_doc.example_service

    RPC
    ---

    .. automethod:: nameko_doc.example_service.BarService.rpc_thing
        :noindex:

    .. automethod:: nameko_doc.example_service.BarService.rpc_2
        :noindex:

    Events
    ------

    .. autoclass:: nameko_doc.example_service.FooEvent
        :noindex:

    Handled Events
    --------------

    ``thing``
    ~~~~~~~~~

    .. automethod:: nameko_doc.example_service.BarService.event_other
        :noindex:

        :Listens to: thing.event

    .. automethod:: nameko_doc.example_service.BarService.event_something
        :noindex:

        :Listens to: thing.something

    ``different``
    ~~~~~~~~~~~~~

    .. automethod:: nameko_doc.example_service.BarService.event_third
        :noindex:

        :Listens to: different.thing

    .. seealso::

        Service Class
            :class:`nameko_doc.example_service.BarService`
        Models
            :mod:`nameko_doc.example_service.models`
