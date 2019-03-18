from __future__ import print_function

import yaml


def main(args):

    with open(args.config) as fle:
        config = yaml.unsafe_load(fle)

    print(yaml.dump(config, default_flow_style=False))
