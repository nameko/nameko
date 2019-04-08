"""Load configuration from YAML file. Allow env var rendering.

E.g.::

    fname = "conf.yaml"
    define = {"MY_PARAM": "is here", "YOUR_PARAM": "is here too"}
    with open(fname, "rb") as config_file:
        setup_config(config_file)
        # from now on, nameko.config is updated
    from nameko import config
    assert "MY_PARAM" in config
    assert "YOUR_PARAM" in config
"""
from __future__ import print_function

import os
import re
import warnings
from functools import partial

import yaml

from nameko import config
from nameko.constants import AMQP_URI_CONFIG_KEY


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
        """,
        re.VERBOSE,
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
        regex.VERBOSE,
    )

IMPLICIT_ENV_VAR_MATCHER = re.compile(
    r"""
        .*          # matches any number of any characters
        \$\{.*\}    # matches any number of any characters
                    # between `${` and `}` literally
        .*          # matches any number of any characters
    """,
    re.VERBOSE,
)


def _replace_env_var(match):
    env_var, default = match.groups()
    value = os.environ.get(env_var, None)
    if value is None:
        # expand default using other vars
        if default is None:
            # regex module return None instead of
            #  '' if engine didn't entered default capture group
            default = ""

        value = default
        while IMPLICIT_ENV_VAR_MATCHER.match(value):  # pragma: no cover
            value = ENV_VAR_MATCHER.sub(_replace_env_var, value)
    return value


def env_var_constructor(loader, node, raw=False):
    raw_value = loader.construct_scalar(node)
    value = ENV_VAR_MATCHER.sub(_replace_env_var, raw_value)
    return value if raw else yaml.safe_load(value)


def setup_yaml_parser():
    yaml.add_constructor("!env_var", env_var_constructor, yaml.UnsafeLoader)
    yaml.add_constructor(
        "!raw_env_var", partial(env_var_constructor, raw=True), yaml.UnsafeLoader
    )
    yaml.add_implicit_resolver(
        "!env_var", IMPLICIT_ENV_VAR_MATCHER, Loader=yaml.UnsafeLoader
    )


def load_config(config_file):
    setup_yaml_parser()
    return yaml.unsafe_load(config_file)


def setup_config(config_file, define=None, broker=None):
    if config_file:
        config.update(load_config(config_file))
    # --broker is deprecated, emit warning if used
    if broker:
        warnings.warn(
            (
                "--broker option is going to be removed. ",
                "Use --config or --define and set AMQP_URI instead.",
            ),
            DeprecationWarning,
        )
        if AMQP_URI_CONFIG_KEY not in config:
            config.update({AMQP_URI_CONFIG_KEY: broker})
    if define:
        config.update(define)
    return config
