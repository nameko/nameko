""" DEPRECATED Nameko built-in dependencies.
"""
import warnings

from nameko import config
from nameko.extensions import DependencyProvider


class Config(DependencyProvider):
    """ DEPRECATED Dependency provider for accessing configuration values.
    """

    def get_dependency(self, worker_ctx):
        warnings.warn("Use ``nameko.config`` instead.", DeprecationWarning)
        return config.copy()
