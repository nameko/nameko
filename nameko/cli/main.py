from __future__ import print_function

import argparse
import os
import re
import yaml

from nameko.exceptions import CommandError, ConfigurationError
from . import commands

ENV_VAR_MATCHER = re.compile(
    r"""
        \$\{       # match characters `${` literally
        ([^}:\s]+) # 1st group: matches any character except `}` or `:`
        :?         # matches the literal `:` character zero or one times
        ([^}]+)?   # 2nd group: matches any character except `}`
        \}         # match character `}` literally
    """, re.VERBOSE
)
IMPLICIT_ENV_VAR_MATCHER = re.compile(
    r"""
        .*          # matches any number of any characters
        \$\{.*\}    # matches any number of any characters
                    # between `${` and `}` literally
        .*          # matches any number of any characters
    """, re.VERBOSE
)


def setup_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    for command in commands.commands:
        command_parser = subparsers.add_parser(
            command.name, description=command.__doc__)
        command.init_parser(command_parser)
        command_parser.set_defaults(main=command.main)
    return parser


def _replace_env_var(match):
    env_var, default = match.groups()
    return os.environ.get(env_var, default)


def _env_var_constructor(loader, node):
    raw_value = loader.construct_scalar(node)
    value = ENV_VAR_MATCHER.sub(_replace_env_var, raw_value)
    use_implicit_scalar_resolver = True
    # PyYAML requires tuple/list value for `implicit` arg in `resolve` method
    # containing two items. Second one is not used so passing `None` here.
    new_tag = loader.resolve(
        yaml.ScalarNode, value, (use_implicit_scalar_resolver, None))
    new_node = yaml.ScalarNode(new_tag, value)
    return loader.yaml_constructors[new_tag](loader, new_node)


def setup_yaml_parser():
    yaml.add_constructor('!env_var', _env_var_constructor)
    yaml.add_implicit_resolver('!env_var', IMPLICIT_ENV_VAR_MATCHER)


def main():
    parser = setup_parser()
    args = parser.parse_args()
    setup_yaml_parser()
    try:
        args.main(args)
    except (CommandError, ConfigurationError) as exc:
        print("Error: {}".format(exc))
