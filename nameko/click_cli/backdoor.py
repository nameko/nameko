from __future__ import print_function

import os
from subprocess import call

from nameko.exceptions import CommandError


def main(backdoor_port, rlwrap):
    for choice in ["netcat", "nc", "telnet"]:
        if os.system("which %s &> /dev/null" % choice) == 0:
            prog = choice
            break
    else:
        raise CommandError("Could not find an installed telnet.")

    host, port = backdoor_port

    cmd = [prog, str(host), str(port)]
    if prog == "netcat":
        # cmd.append("--close")
        cmd.append("-C")
    if rlwrap is None:
        rlwrap = os.system("which rlwrap &> /dev/null") == 0
    if rlwrap:
        cmd.insert(0, "rlwrap")
    try:
        print(f"cmd: {cmd}")
        if call(cmd) != 0:
            raise CommandError("Backdoor unreachable on {}".format(backdoor_port))
    except (EOFError, KeyboardInterrupt):
        print()
        if choice == "telnet" and rlwrap:
            call(["reset"])
