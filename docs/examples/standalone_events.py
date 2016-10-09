from nameko.standalone.events import event_dispatcher

config = {
    'AMQP_URI': AMQP_URI  # e.g. "pyamqp://guest:guest@localhost"
}

dispatch = event_dispatcher(config)
dispatch("service_a", "event_type", "payløad")
