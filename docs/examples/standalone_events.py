from nameko import config_setup
from nameko.standalone.events import event_dispatcher

config_setup({
    'AMQP_URI': AMQP_URI  # e.g. "pyamqp://guest:guest@localhost"
})

dispatch = event_dispatcher()
dispatch("service_a", "event_type", "payløad")
