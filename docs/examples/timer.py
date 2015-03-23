from nameko.timer import timer

class Service(object):
    name ="service"

    @timer(interval=5)
    def ping(self):
        # method executed every 5 seconds
        print "pong"
