from __future__ import print_function

import argparse
import os
import re
import yaml

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


def _replace_env_var(match):
    env_var, _, default = match.groups()
    return os.environ.get(env_var, default)


def _env_var_constructor(loader, node):
    value = loader.construct_scalar(node)
    return re.compile(
        r'\$\{([a-zA-Z][^}:\s]+)(:([^}]+))?\}'
    ).sub(_replace_env_var, value)


def setup_yaml_parser():
    yaml.add_constructor('!env_var', _env_var_constructor)
    yaml.add_implicit_resolver('!env_var', re.compile(r'.*\$\{.*\}.*'))


def main():
    parser = setup_parser()
    args = parser.parse_args()
    setup_yaml_parser()
    try:
        args.main(args)
    except (CommandError, ConfigurationError) as exc:
        print("Error: {}".format(exc))
