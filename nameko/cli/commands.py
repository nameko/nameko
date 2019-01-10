"""Commands are defined in here, with imports inline, to avoid triggering
imports from other subcommands (e.g. `run` will cause an eventlet monkey-patch
which we don't want for `shell`)

"""
from .actions import FlagAction


class Command(object):
    name = None

    @staticmethod
    def init_parser(parser):
        raise NotImplementedError  # pragma: no cover

    @staticmethod
    def main(args):
        # import inline to avoid triggering imports from other subcommands
        raise NotImplementedError  # pragma: no cover


class Backdoor(Command):
    """Connect to a nameko backdoor.

    If a backdoor is running this will connect to a remote shell.  The
    runner is generally available as `runner`.
    """

    name = 'backdoor'

    @staticmethod
    def init_parser(parser):
        parser.add_argument(
            'target', metavar='[host:]port',
            help="(host and) port to connect to",
        )
        parser.add_argument(
            '--rlwrap', dest='rlwrap', action=FlagAction,
            help='Use rlwrap')
        parser.set_defaults(feature=True)
        return parser

    @staticmethod
    def main(args):
        from .backdoor import main
        main(args)


class ShowConfig(Command):
    """ Output as YAML string the configuration that would be passed to a
    service.

    Useful for viewing config files that load values from environement
    variables.
    """

    name = 'show-config'

    @staticmethod
    def init_parser(parser):

        parser.add_argument(
            '--config', default='config.yaml',
            help='The YAML configuration file')

        return parser

    @staticmethod
    def main(args):
        from .show_config import main
        main(args)


class Run(Command):
    """Run nameko services.  Given a python path to a module containing one or
    more nameko services, will host and run them. By default this will try to
    find classes that look like services (anything with nameko entrypoints),
    but a specific service can be specified via
    ``nameko run module:ServiceClass``.
    """

    name = 'run'

    @staticmethod
    def init_parser(parser):
        parser.add_argument(
            'services', nargs='+',
            metavar='module[:service class]',
            help='python path to one or more service classes to run')

        parser.add_argument(
            '--config', default='',
            help='The YAML configuration file')

        parser.add_argument(
            '--broker', default='pyamqp://guest:guest@localhost',
            help='RabbitMQ broker url')

        parser.add_argument(
            '--backdoor-port', type=int,
            help='Specify a port number to host a backdoor, which can be'
            ' connected to for an interactive interpreter within the running'
            ' service process using `nameko backdoor`.')

        return parser

    @staticmethod
    def main(args):
        from .run import main
        main(args)


class Shell(Command):
    """Launch an interactive python shell for working with remote nameko
    services.

    This is a regular interactive interpreter, with a special module ``n``
    added to the built-in namespace, providing ``n.rpc`` and
    ``n.dispatch_event``.
    """

    name = 'shell'

    SHELLS = ['bpython', 'ipython', 'plain']

    @classmethod
    def init_parser(cls, parser):
        parser.add_argument(
            '--broker', default='pyamqp://guest:guest@localhost',
            help='RabbitMQ broker url')
        parser.add_argument(
            '--interface', choices=cls.SHELLS,
            help='Specify an interactive interpreter interface.'
                 ' (Ignored if not in TTY mode)')
        parser.add_argument(
            '--config', default='',
            help='The YAML configuration file')
        return parser

    @staticmethod
    def main(args):
        from .shell import main
        main(args)


commands = Command.__subclasses__()  # pylint: disable=E1101
