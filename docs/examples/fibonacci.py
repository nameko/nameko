from collections import deque

from nameko.extensions import DependencyProvider
from nameko.rpc import rpc


class DataStore(DependencyProvider):

    def setup(self):
        self.data = deque((0, 1), maxlen=2)

    def get_dependency(self, worker_ctx):
        return self.data


class Fibonacci(object):
    name = "fibonacci"

    store = DataStore()

    @rpc
    def next(self):

        n2 = self.store.popleft()
        n1 = self.store[0]

        n = n1 + n2
        self.store.append(n)
        return n
