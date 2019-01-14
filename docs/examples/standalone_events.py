from nameko import config
from nameko.standalone.events import event_dispatcher

config.setup({
    'AMQP_URI': AMQP_URI  # e.g. "pyamqp://guest:guest@localhost"
})

dispatch = event_dispatcher()
dispatch("service_a", "event_type", "payløad")
