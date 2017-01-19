.. _built_in_dependency_providers:

Built-in Dependency Providers
=============================

Nameko includes some commonly used :ref:`dependency providers <dependency_injection>`. This section introduces them and gives brief examples of their usage.

.. _config_dependency_provider:

Config
------

Config is a simple dependency provider that gives services read-only access to configuration values at run time, see :ref:`running_a_service`.

.. literalinclude:: examples/config_dependency_provider.py
