from .sqs_receive import receive


class SqsService(object):
    name = "sqs-service"

    @receive('https://sqs.eu-west-1.amazonaws.com/123456789012/nameko-sqs')
    def handle_sqs_message(self, body):
        """ This method is called by the `receive` entrypoint whenever
        a message sent to the given SQS queue.
        """
        print(body)
        return body
