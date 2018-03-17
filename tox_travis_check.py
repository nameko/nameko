"""
Make sure environments specified in tox.ini match the ones in .travis.yml
"""
from __future__ import print_function

import itertools
import re
import sys

# uses regexes instead of parsers to obviate need for dependencies


# from tox/config.py (not changed in a few years)
def _expand_envstr(envstr):
    # split by commas not in groups
    tokens = re.split(r'((?:\{[^}]+\})+)|,', envstr)
    envlist = [''.join(g).strip()
               for k, g in itertools.groupby(tokens, key=bool) if k]

    def expand(env):
        tokens = re.split(r'\{([^}]+)\}', env)
        parts = [token.split(',') for token in tokens]
        return [''.join(variant) for variant in itertools.product(*parts)]

    return mapcat(expand, envlist)


def mapcat(f, seq):
    return list(itertools.chain.from_iterable(map(f, seq)))


def get_travis_envs():
    with open('.travis.yml') as handle:
        config = handle.read()
    envs = re.findall('TOX_ENV=(.*)', config)
    return envs


def get_tox_envs():
    with open('tox.ini') as handle:
        config = handle.read()
    match = re.search('envlist\s*=\s*(.*)', config)
    assert match, 'tox envlist not found'
    (envlist, ) = match.groups()
    envs = _expand_envstr(envlist)
    return envs


def main():
    travis_envs = set(get_travis_envs())
    tox_envs = set(get_tox_envs())
    travis_not_tox = travis_envs - tox_envs
    tox_not_travis = tox_envs - travis_envs
    status = 0
    if travis_not_tox:
        print('Envs in .travis.yml missing from tox.ini: {}'.format(
            ', '.join(travis_not_tox)
        ))
        status = 1
    if tox_not_travis:
        print('Envs in tox.ini missing from .travis.yml: {}'.format(
            ', '.join(tox_not_travis)
        ))
        status = 1

    sys.exit(status)


if __name__ == '__main__':
    main()
