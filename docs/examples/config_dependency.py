from nameko.dependencies import Config
from nameko.rpc import rpc


class Service:

    name = "test_config"

    config = Config()

    @rpc
    def get_max_workers(self):
        return self.config.get("max_workers")
