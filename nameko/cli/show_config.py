from __future__ import print_function

import yaml


def main(args):
    config = {}
    for config_file in args.config:
        with open(config_file) as fle:
            config.update(yaml.load(fle))

    print(yaml.dump(config, default_flow_style=False))
