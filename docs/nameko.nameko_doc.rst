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

    nameko_doc -s <source_dir> -o <output_dir>

The above command will work with packages inside ``<source_dir>``, producing
output as Sphinx source .rst files in ``<output_dir>``. Or, use
``nameko_doc --help`` for further options.

Pre-requisites
--------------

``nameko_doc`` uses configuration from a ``setup.cfg`` file -- you'll either
need to have one of pass in an alternative file through the ``--config``
option. An example file is included.

How does this compare to ``sphinx-apidoc``?
-------------------------------------------

``sphinx-apidoc`` will attempt to produce ``.rst`` files for an entire module
path; ``nameko_doc`` performs introspection to produce ``.rst`` files for just
service related content. At onefinestay, we use the two together -- standard
API docs are consumed by the service engineers, and ``nameko_doc`` docs are used
by front-end developers.

Identifying Services
--------------------

``nameko_doc`` provides a default 'Extractor' that will be able to find service
classes and methods in a typical usage scenario -- note, you can pass in an
alternative extractor class through ``--extractor``. You're encouraged to use
this if your code organisation doesn't match up with the default expectations.

Default Extractor
~~~~~~~~~~~~~~~~~

Firstly, the default extractor will only look for service classes defined in
two locations:

#. The root of a package
#. ``controller.py`` within a package.

To identify service methods, `nameko_doc` attempts two approaches:

#. If the method has been marked with nameko's built-in ``@rpc``, or something
   that inherits from it, it's a service method.
#. Otherwise, we look at the name of any decorators applied to a method and
   match it against a list of names supplied in the config.

To find service classes, we just check for matching decorator names. The
example config file shows how to use ``class_instructions``,
``method_instructions`` and ``trigger_names`` to configure name matching.


nameko.nameko_doc package
=========================

Subpackages
-----------

.. toctree::

    nameko.nameko_doc.extractors
    nameko.nameko_doc.renderers

Submodules
----------

nameko.nameko_doc.main module
-----------------------------

.. automodule:: nameko.nameko_doc.main
    :members:
    :undoc-members:
    :show-inheritance:


nameko.nameko_doc.processor module
----------------------------------

.. automodule:: nameko.nameko_doc.processor
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
