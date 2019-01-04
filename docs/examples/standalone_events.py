from nameko import setup_config
from nameko.standalone.events import event_dispatcher

setup_config({
    'AMQP_URI': AMQP_URI  # e.g. "pyamqp://guest:guest@localhost"
})

dispatch = event_dispatcher()
dispatch("service_a", "event_type", "payløad")
