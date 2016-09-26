from nameko.timer import timer

class Service:
    name ="service"

    @timer(interval=1)
    def ping(self):
        # method executed every second
        print("pong")
