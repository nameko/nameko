from nameko.extensions import DependencyProvider

import boto3

class SqsSend(DependencyProvider):

    def __init__(self, url, region="eu-west-1", **kwargs):
        self.url = url
        self.region = region
        super(SqsSend, self).__init__(**kwargs)

    def setup(self):
        self.client = boto3.client('sqs', region_name=self.region)

    def get_dependency(self, worker_ctx):

        def send_message(payload):
            # assumes boto client is thread-safe for this action, because it
            # happens inside the worker threads
            self.client.send_message(
                QueueUrl=self.url,
                MessageBody=payload
            )
        return send_message
