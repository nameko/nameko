import argparse

from . import run, shell

def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    for module in [run, shell]:
        name = module.__name__.split('.')[-1]
        module_parser = subparsers.add_parser(
            name, description=module.__doc__)
        module.init_parser(module_parser)
        module_parser.set_defaults(main=module.main)

    args = parser.parse_args()
    args.main(args)
