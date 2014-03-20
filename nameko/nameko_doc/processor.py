import logging
from path import path
from .method_extractor import MethodExtractor
from .rst_render import RstPagePrinter


log = logging.getLogger(__name__)


def get_function_from_path(function_path):
    pieces = function_path.split('.')
    mod_path = '.'.join(pieces[:-1])
    function = __import__(mod_path)
    for fetch in pieces[1:]:
        function = getattr(function, fetch)
    return function


class Processor(object):
    def __init__(self, output=None, service_loader_function=None):
        self.output = path(output) if output else output
        self.service_loader_function = get_function_from_path(
            service_loader_function
        ) if service_loader_function else None

    @classmethod
    def from_parsed_args(cls, parsed):
        return cls(
            output=parsed.output,
            service_loader_function=parsed.service_loader,
        )

    def extract_docs(self):
        if not self.output.exists():
            self.output.mkdir_p()
        else:
            self.output.rmtree()
            self.output.mkdir()

        extractor = MethodExtractor(
            self.service_loader_function
        )
        collection = extractor.extract()

        printer = RstPagePrinter(
            self.output
        )
        with printer:
            log.debug(collection.services)
            collection.render(printer)
