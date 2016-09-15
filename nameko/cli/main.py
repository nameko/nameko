from __future__ import print_function

import argparse
import os
import re
import yaml

from nameko.exceptions import CommandError, ConfigurationError
from . import backdoor, run, shell


ENV_VAR_PATTERN = re.compile(
    r'\$\{([a-zA-Z][^}:\s]+)(:([^}]+))?\}', re.MULTILINE | re.DOTALL
)
IMPLICIT_MATCH_PATTERN = re.compile(r'.*\$\{.*\}.*', re.MULTILINE)


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


def replace_env_var(match):
    env_var, _, default = match.groups()
    if default == 'null':
        default = None
    return os.environ.get(env_var, default)


def env_var_constructor(loader, node):
    value = loader.construct_scalar(node)
    return ENV_VAR_PATTERN.sub(replace_env_var, value)


def setup_yaml_parser():
    yaml.add_constructor('!env_var', env_var_constructor)
    yaml.add_implicit_resolver("!env_var", IMPLICIT_MATCH_PATTERN)


def main():
    parser = setup_parser()
    args = parser.parse_args()
    setup_yaml_parser()
    try:
        args.main(args)
    except (CommandError, ConfigurationError) as exc:
        print("Error: {}".format(exc))
