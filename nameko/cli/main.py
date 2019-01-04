from __future__ import print_function

import argparse
import os
import re
import warnings
from functools import partial

import yaml

from nameko import config
from nameko.constants import AMQP_URI_CONFIG_KEY
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
    subparsers = parser.add_subparsers()

    for command in commands.commands:
        command_parser = subparsers.add_parser(
            command.name, description=command.__doc__,
            conflict_handler='resolve')
        command.init_parser(command_parser)
        command_parser.set_defaults(main=command.main)
        command_parser.add_argument(
            '--config',
            help='The YAML configuration file')
        command_parser.add_argument(
            '-d', '--define',
            type=parse_config_option, action='append', metavar='KEY=VALUE',
            help='Set config entry. Overrides value loaded from config file.'
            ' Can be used multiple times.'
            ' Example: --define AMQP_URI=pyamqp://guest:guest@localhost')

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
    yaml.add_constructor('!env_var', env_var_constructor)
    yaml.add_constructor('!raw_env_var',
                         partial(env_var_constructor, raw=True))
    yaml.add_implicit_resolver('!env_var', IMPLICIT_ENV_VAR_MATCHER)


def load_config(config_path):
    with open(config_path) as fle:
        return yaml.load(fle)


def parse_config_option(text):
    if '=' in text:
        key, value = text.strip().split('=', 1)
        return key, yaml.load(value)
    else:
        return text, True


def setup_config(args):
    setup_yaml_parser()
    if args.config:
        config.update(load_config(args.config))
    if hasattr(args, "broker") and args.broker:
        warnings.warn(
            (
                "--broker option is going to be removed. ",
                "Use --config or --define and set AMQP_URI instead."
            ),
            DeprecationWarning
        )
        if AMQP_URI_CONFIG_KEY not in config:
            config.update({AMQP_URI_CONFIG_KEY: args.broker})
    if args.define:
        config.update(args.define)


def main():
    parser = setup_parser()
    args = parser.parse_args()
    setup_config(args)
    try:
        args.main(args)
    except (CommandError, ConfigurationError) as exc:
        print("Error: {}".format(exc))
