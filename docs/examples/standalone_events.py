from nameko.standalone.events import event_dispatcher

config = {
    'AMQP_URI': 'amqp://guest:guest@localhost/nameko'
}

dispatch = event_dispatcher(config)
dispatch("service_a", "payload")
