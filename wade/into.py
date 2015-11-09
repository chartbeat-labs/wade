"""Utility functions for turning command definitions into services."""

import signal
from functools import partial

import pyuv

from wade import chain
from wade.chorus import Node
from wade.chorus import CallInterface


def standard_setup(my_id, port, store):
    """Sets up and return a pyuv loop to run as the main server.

    Sets up signal handling in addition to the call interface and node
    handler.

    """

    loop = pyuv.Loop()

    call_interface = CallInterface(loop)
    handler = chain.Handler(my_id, store, call_interface)
    node = Node(loop, port, call_interface, handler)

    signal_stop = pyuv.Signal(loop)
    signal_stop.start(
        partial(_unwind_loop, [call_interface, node], signal_stop),
        signal.SIGINT,
    )

    return loop

def _unwind_loop(registrants, signal_stop, handle, signum):
    """Signal callback that unwinds the given registrants loop hooks."""

    for r in registrants:
        r.unwind_loop()

    signal_stop.close()
