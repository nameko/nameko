"""Provide custom code `interact` for interactive nameko shell.
"""
from __future__ import absolute_import

from code import InteractiveConsole


# This class is covered by tests run in subprocess hence the no cover
class RaisingInteractiveConsole(InteractiveConsole):  # pragma: no cover
    """ Custom InterativeConsole class that allows raising exception if needed.
    """

    def __init__(
        self, locals=None, filename="<console>", raise_expections=False
    ):
        InteractiveConsole.__init__(self, locals=locals, filename=filename)
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


# This function is covered by tests run in subprocess hence the no cover
def interact(
    banner=None, local=None, raise_expections=False
):  # pragma: no cover
    try:
        import readline  # noqa: F401
    except ImportError:
        pass

    console = RaisingInteractiveConsole(
        locals=local, raise_expections=raise_expections
    )
    console.interact(banner=banner)
