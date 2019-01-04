from __future__ import print_function

import yaml

from nameko import config


def main(args):
    print(yaml.dump(config, default_flow_style=False))
