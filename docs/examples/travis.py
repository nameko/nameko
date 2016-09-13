import requests

from nameko.extensions import DependencyProvider
from nameko.rpc import rpc

URL_TEMPLATE = "https://api.travis-ci.org/repos/{}/{}"


class ApiWrapper:

    def __init__(self, session):
        self.session = session

    def repo_status(self, owner, repo):
        url = URL_TEMPLATE.format(owner, repo)
        return self.session.get(url).json()


class TravisWebservice(DependencyProvider):

    def setup(self):
        self.session = requests.Session()

    def get_dependency(self, worker_ctx):
        return ApiWrapper(self.session)


class Travis:
    name = "travis_service"

    webservice = TravisWebservice()

    @rpc
    def status_message(self, owner, repo):
        status = self.webservice.repo_status(owner, repo)
        outcome = "passing" if status['last_build_result'] else "failing"

        return "Project {repo} {outcome} since {timestamp}.".format(
            repo=status['slug'],
            outcome=outcome,
            timestamp=status['last_build_finished_at']
        )
