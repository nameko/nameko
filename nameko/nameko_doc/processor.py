import ConfigParser
import importlib
import logging
from path import path
from nameko.nameko_doc.renderers.rst import RstDirectoryRenderer


DEFAULT_EXTRACTOR_CLS_PATH = 'nameko.nameko_doc.extractors.AstExtractor'


log = logging.getLogger(__name__)


class Processor(object):
    def __init__(self, source=None, output=None,
                 config_file=None,
                 extractor_class_path=None):
        self.source = path(source) if source else source
        self.output = path(output) if output else output
        self._config_file = config_file
        self.extractor_class_path = (extractor_class_path or
                                     DEFAULT_EXTRACTOR_CLS_PATH)

    @classmethod
    def from_parsed_args(cls, parsed):
        return cls(
            source=parsed.source,
            output=parsed.output,
            config_file=parsed.config_file,
            extractor_class_path=parsed.extractor_class,
        )

    def extract_docs(self):
        if not self.output.exists():
            self.output.mkdir_p()
        else:
            self.output.rmtree()
            self.output.mkdir()

        extractor_cls = self._get_extractor_cls()
        config_parser = self._config_parser()
        extractor = extractor_cls(
            self.source,
            config_parser
        )
        with extractor:
            collection = extractor.extract()

        # TODO configurable renderer
        renderer = RstDirectoryRenderer(
            self.output,
            config_parser,
        )
        with renderer:
            log.debug(collection.services)
            collection.render(renderer)

    def _config_parser(self):
        potentials = (
            self._config_file,
            'setup.cfg',
        )
        for potential in potentials:
            if potential and path(potential).exists():
                config = ConfigParser.SafeConfigParser()
                config.read(potential)
                return config

        # The extractor class might not need a config anyway.
        return None

    def _get_extractor_cls(self):
        mod_name, cls_name = self.extractor_class_path.rsplit('.', 1)
        mod = importlib.import_module(mod_name)
        return getattr(mod, cls_name)
