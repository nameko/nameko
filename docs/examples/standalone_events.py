from nameko.standalone.events import event_dispatcher

dispatch = event_dispatcher(uri=AMQP_URI)  # e.g. "pyamqp://guest:guest@localhost"
dispatch("service_a", "event_type", "payløad")
