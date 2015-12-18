"""Utility functions for turning command definitions into services."""

import signal
from functools import partial

import pyuv

from wade import chain
from wade.chorus import Node
from wade.chorus import CallInterface


def standard_setup(my_id, port, store, allow_dangerous_debugging):
    """Sets up and return a pyuv loop to run as the main server.

    Sets up signal handling in addition to the call interface and node
    handler.

    """

    loop = pyuv.Loop()
    call_interface = CallInterface(loop)
    handler = chain.Handler(
        my_id,
        store,
        call_interface,
        allow_dangerous_debugging,
    )

    return node_setup(port, handler, call_interface, loop)


def node_setup(port, handler, call_interface=None, loop=None):
    """Starts up just the chorus communication handler."""

    if not loop:
        loop = pyuv.Loop()
    if not call_interface:
        call_interface = CallInterface(loop)

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
