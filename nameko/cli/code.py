from code import InteractiveConsole


class RaisingInteractiveConsole(InteractiveConsole):
    """ Custom InterativeConsole class that allows raising exception if needed.
    """

    def __init__(
        self, locals=None, filename="<console>", raise_expections=False
    ):
        super(RaisingInteractiveConsole, self).__init__(
            locals=locals, filename=filename
        )
        self.raise_expections = raise_expections

    def runcode(self, code):
        try:
            exec(code, self.locals)
        except SystemExit:
            raise
        except:
            self.showtraceback()
            if self.raise_expections:
                raise


def interact(banner=None, local=None, raise_expections=False):
    try:
        import readline  # noqa: F401
    except ImportError:
        pass

    console = RaisingInteractiveConsole(
        locals=local, raise_expections=raise_expections
    )
    console.interact(banner=banner)
