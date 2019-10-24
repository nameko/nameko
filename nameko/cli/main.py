from __future__ import print_function

import argparse
import os
import re
from functools import partial

import yaml
from pkg_resources import get_distribution

from nameko.exceptions import CommandError, ConfigurationError

from . import commands


try:
    import regex
except ImportError:  # pragma: no cover
    ENV_VAR_MATCHER = re.compile(
        r"""
            \$\{       # match characters `${` literally
            ([^}:\s]+) # 1st group: matches any character except `}` or `:`
            :?         # matches the literal `:` character zero or one times
            ([^}]+)?   # 2nd group: matches any character except `}`
            \}         # match character `}` literally
        """, re.VERBOSE
    )
else:  # pragma: no cover
    ENV_VAR_MATCHER = regex.compile(
        r"""
        \$\{                #  match ${
        (                   #  first capturing group: variable name
            [^{}:\s]+       #  variable name without {,},: or spaces
        )
        (?:                 # non capturing optional group for value
            :               # match :
            (               # 2nd capturing group: default value
                (?:         # non capturing group for OR
                    [^{}]   # any non bracket
                |           # OR
                    \{      # literal {
                    (?2)    # recursive 2nd capturing group aka ([^{}]|{(?2)})
                    \}      # literal }
                )*          #
            )
        )?
        \}                  # end of macher }
        """,
        regex.VERBOSE
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
    parser.add_argument(
        '-v',
        '--version',
        action='version',
        version=get_distribution('nameko').version
    )
    subparsers = parser.add_subparsers()

    for command in commands.commands:
        command_parser = subparsers.add_parser(
            command.name, description=command.__doc__)
        command.init_parser(command_parser)
        command_parser.set_defaults(main=command.main)
    return parser


def _replace_env_var(match):
    env_var, default = match.groups()
    value = os.environ.get(env_var, None)
    if value is None:
        # expand default using other vars
        if default is None:
            # regex module return None instead of
            #  '' if engine didn't entered default capture group
            default = ''

        value = default
        while IMPLICIT_ENV_VAR_MATCHER.match(value):  # pragma: no cover
            value = ENV_VAR_MATCHER.sub(_replace_env_var, value)
    return value


def env_var_constructor(loader, node, raw=False):
    raw_value = loader.construct_scalar(node)
    value = ENV_VAR_MATCHER.sub(_replace_env_var, raw_value)
    return value if raw else yaml.safe_load(value)


def setup_yaml_parser():
    yaml.add_constructor('!env_var', env_var_constructor, yaml.UnsafeLoader)
    yaml.add_constructor(
        '!raw_env_var',
        partial(env_var_constructor, raw=True),
        yaml.UnsafeLoader
    )
    yaml.add_implicit_resolver(
        '!env_var', IMPLICIT_ENV_VAR_MATCHER, Loader=yaml.UnsafeLoader
    )


def main():
    parser = setup_parser()
    args = parser.parse_args()
    setup_yaml_parser()
    try:
        args.main(args)
    except (CommandError, ConfigurationError) as exc:
        print("Error: {}".format(exc))
