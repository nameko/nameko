from copy import deepcopy

import kombu.serialization

from nameko.constants import (
    ACCEPT_CONFIG_KEY, DEFAULT_SERIALIZER, SERIALIZER_CONFIG_KEY,
    SERIALIZERS_CONFIG_KEY
)
from nameko.exceptions import ConfigurationError
from nameko.utils import import_from_path


def setup(config):

    serializers = deepcopy(config.get(SERIALIZERS_CONFIG_KEY, {}))
    for name, kwargs in serializers.items():
        encoder = import_from_path(kwargs.pop('encoder'))
        decoder = import_from_path(kwargs.pop('decoder'))
        kombu.serialization.register(
            name, encoder=encoder, decoder=decoder, **kwargs)

    serializer = config.get(SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER)

    accept = config.get(ACCEPT_CONFIG_KEY, [serializer])

    for name in [serializer] + accept:
        try:
            kombu.serialization.registry.name_to_type[name]
        except KeyError:
            raise ConfigurationError(
                'Please register a serializer for "{}" format'.format(name))

    return serializer, accept
