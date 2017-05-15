""" Nameko built-in dependencies.
"""

from nameko.extensions import DependencyProvider


class Config(DependencyProvider):
    """ Dependency provider for accessing configuration values.
    """

    def __init__(self, section=None):
        self.section = section

    def get_dependency(self, worker_ctx):
        config = self.container.config.copy()
        if self.section:
            return config[self.section]
        return config
