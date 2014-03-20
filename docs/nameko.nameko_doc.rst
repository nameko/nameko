Nameko Doc
==========

The ``nameko_doc`` package provides library support for building tools designed
to help produce documentation source files for
`Nameko <https://github.com/onefinestay/nameko>`_ based services, with a focus
on caller-oriented documentation. The aim is to help produce documentation
tightly aimed at a developer calling into a service cluster, specifically
including service RPC methods and leaving out things like unexposed public
methods.

Using the library
-----------------

To use the library, construct an instance of
:class:`nameko.nameko_doc.processor.ServiceDocProcessor`, providing a location
to store generated ``.rst`` files, and a function which will return a list of
service classes.

From there, we will check for any methods in each service class
that have been marked with nameko's built in ``@rpc`` or something that
inherits from it.

How does this compare to ``sphinx-apidoc``?
-------------------------------------------

``sphinx-apidoc`` will attempt to produce ``.rst`` files for an entire module
path; ``nameko_doc`` can be used to produce ``.rst`` files for just
service related content. At onefinestay, we use the two together -- standard
API docs are consumed by the service engineers, and ``nameko_doc`` docs are used
by front-end developers.

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

    .. seealso::

        Service Class
            :class:`nameko_doc.example_service.BarService`
