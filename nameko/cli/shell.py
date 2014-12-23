def main(args):
    print "shell", args


def init_parser(parser):
    parser.add_argument(
        '--broker', default='amqp://guest:guest@localhost:5672/nameko',
        help='RabbitMQ broker url')
    return parser
