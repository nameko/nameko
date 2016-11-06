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
    value = loader.construct_scalar(node)
    return ENV_VAR_MATCHER.sub(_replace_env_var, value)


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
