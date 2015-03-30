from __future__ import print_function

import argparse

from nameko.exceptions import CommandError, ConfigurationError
from . import backdoor, run, shell


def setup_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    for module in [backdoor, run, shell]:
        name = module.__name__.split('.')[-1]
        module_parser = subparsers.add_parser(
            name, description=module.__doc__)
        module.init_parser(module_parser)
        module_parser.set_defaults(main=module.main)
    return parser


def main():
    parser = setup_parser()
    args = parser.parse_args()
    try:
        args.main(args)
    except (CommandError, ConfigurationError) as exc:
        print("Error: {}".format(exc))
