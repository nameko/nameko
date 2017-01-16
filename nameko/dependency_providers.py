""" Nameko built-in dependencies.
"""

from nameko.extensions import DependencyProvider


class Config(DependencyProvider):
    """ Dependency provider for accessing configuration values.
    """

    def get_dependency(self, worker_ctx):
        return self.container.config.copy()
