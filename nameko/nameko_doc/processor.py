import logging
from path import Path
from .method_extractor import MethodExtractor
from .rst_render import RstPagePrinter


log = logging.getLogger(__name__)


class ServiceDocProcessor(object):
    def __init__(self, output, service_loader_function):
        self.output = Path(output)
        self.service_loader_function = service_loader_function

    def write_docs(self):
        if not self.output.exists():
            self.output.mkdir_p()
        elif self.output.files():
            raise ValueError('The provided output folder is not empty.')

        extractor = MethodExtractor(
            self.service_loader_function,
        )
        collection = extractor.extract()

        printer = RstPagePrinter(
            self.output
        )
        with printer:
            log.debug(collection.services)
            collection.render(printer)
