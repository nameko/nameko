"""Connect to a nameko backdoor.

If a backdoor is running this will connect to a remote shell.  The
runner is generally available as `runner`.
"""
from __future__ import print_function

import os
from subprocess import call

from nameko.exceptions import CommandError
from .actions import FlagAction


def main(args):
    for choice in ['netcat', 'nc', 'telnet']:
        if os.system('which %s &> /dev/null' % choice) == 0:
            prog = choice
            break
    else:
        raise CommandError('Could not find an installed telnet.')

    target = args.target
    if ':' in target:
        host, port = target.split(':', 1)
    else:
        host, port = 'localhost', target

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
            raise CommandError(
                'Backdoor unreachable on {}'.format(target)
            )
    except (EOFError, KeyboardInterrupt):
        print()
        if choice == 'telnet' and rlwrap:
            call(['reset'])


def init_parser(parser):
    parser.add_argument(
        'target', metavar='[host:]port', help="(host and) port to connect to")
    parser.add_argument(
        '--rlwrap', dest='rlwrap', action=FlagAction,
        help='Use rlwrap')
    parser.set_defaults(feature=True)
    return parser
