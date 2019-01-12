from __future__ import print_function

import yaml

from nameko import config


def main(args):
    print(yaml.dump(config.data, default_flow_style=False))
