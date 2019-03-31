import argparse
import re


class FlagAction(argparse.Action):
    # From http://bugs.python.org/issue8538

    def __init__(self, option_strings, dest, default=None,
                 required=False, help=None, metavar=None,
                 positive_prefixes=['--'], negative_prefixes=['--no-']):
        self.positive_strings = set()
        self.negative_strings = set()
        for string in option_strings:
            assert re.match(r'--[A-z]+', string)
            suffix = string[2:]
            for positive_prefix in positive_prefixes:
                self.positive_strings.add(positive_prefix + suffix)
            for negative_prefix in negative_prefixes:
                self.negative_strings.add(negative_prefix + suffix)
        strings = list(self.positive_strings | self.negative_strings)
        super(FlagAction, self).__init__(
            option_strings=strings, dest=dest,
            nargs=0, const=None, default=default, type=bool, choices=None,
            required=required, help=help, metavar=metavar)

    def __call__(self, parser, namespace, values, option_string=None):
        if option_string in self.positive_strings:
            setattr(namespace, self.dest, True)
        else:
            setattr(namespace, self.dest, False)
