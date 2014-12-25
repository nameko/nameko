import argparse

from . import backdoor, run, shell
from .exceptions import CommandError


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    for module in [backdoor, run, shell]:
        name = module.__name__.split('.')[-1]
        module_parser = subparsers.add_parser(
            name, description=module.__doc__)
        module.init_parser(module_parser)
        module_parser.set_defaults(main=module.main)

    args = parser.parse_args()
    try:
        args.main(args)
    except CommandError as exc:
        print "Error: {}".format(exc)
