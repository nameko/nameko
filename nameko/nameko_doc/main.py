#!/usr/bin/env python
"""Program entry point"""

from __future__ import print_function

import argparse
import sys

from nameko import metadata
from nameko.nameko_doc.processor import Processor


def main(argv):
    """Program entry point.

    :param argv: command-line arguments
    :type argv: :class:`list`
    """
    author_strings = []
    for name, email in zip(metadata.authors, metadata.emails):
        author_strings.append('Author: {0} <{1}>'.format(name, email))

    epilog = '''
{project} {version}

{authors}
URL: <{url}>
'''.format(
        project='Nameko Service Doc',
        version=metadata.version,
        authors='\n'.join(author_strings),
        url=metadata.url)

    arg_parser = argparse.ArgumentParser(
        prog=argv[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description='Generates Sphinx documentation source files suitable '
                    'for external service clients.',
        epilog=epilog)
    arg_parser.add_argument(
        '-V', '--version',
        action='version',
        version='Nameko Service Doc {0}'.format(metadata.version))
    arg_parser.add_argument(
        '-o', '--output',
        action='store',
        dest='output',
        required=True,
        help='Location to output rendered content.'
    )
    arg_parser.add_argument(
        '-s', '--service-loader',
        action='store',
        dest='service_loader',
        required=True,
        help='Dotted path to function returning service name, module path and '
             'service classes.'
    )

    parsed = arg_parser.parse_args(args=argv[1:])

    print(epilog)

    processor = Processor.from_parsed_args(parsed)
    processor.extract_docs()

    return 0


def entry_point():   # pragma: no cover
    """Zero-argument entry point for use with setuptools/distribute."""
    raise SystemExit(main(sys.argv))


if __name__ == '__main__':  # pragma: no cover
    entry_point()
