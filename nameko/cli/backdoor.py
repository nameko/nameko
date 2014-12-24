"""Connect to a nameko backdoor.

If a backdoor is running this will connect to a remote shell.  The
runner is generally available as `runner`.  Note that this can do
great damage if used incorrectly, so be careful!
"""

import os
from subprocess import call

from .actions import FlagAction
from .exceptions import CommandError


def main(args):
    for choice in ['netcat', 'nc', 'telnet']:
        if os.system('which %s &> /dev/null' % choice) == 0:
            prog = choice
            break
    else:
        raise CommandError('Could not find an installed telnet.')

    host = args.host
    port = args.port
    rlwrap = args.rlwrap

    cmd = [prog, str(host), str(port)]
    if prog == 'netcat':
        cmd.append('--close')
    if rlwrap is None:
        rlwrap = os.system('which rlwrap &> /dev/null') == 0
    if rlwrap:
        cmd.insert(0, 'rlwrap')
    try:
        if call(cmd) != 0:
            raise CommandError('error: backdoor unreachable')
    except (EOFError, KeyboardInterrupt):
        print
        if rlwrap:
            call(['reset'])


def init_parser(parser):
    parser.add_argument(
        '--host', default='127.0.0.1', help='host to connect to')
    parser.add_argument(
        '-p', '--port', help='port to connect to')
    parser.add_argument(
        '--rlwrap', dest='rlwrap', action=FlagAction,
        help='Use rlwrap')
    parser.set_defaults(feature=True)
    return parser
