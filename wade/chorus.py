
"""An peer communcation + event loop implementation which allows for
single threaded asynchronous communication amongst a set of equal
peers.

"""

import socket
import logging
from functools import partial
from collections import namedtuple

import pyuv
import msgpack


OK = 'ok'
ERR = 'err'
TIMEOUT = 'timeout'


class NodeError(Exception):
    pass

# Represents an incoming connection.
Incoming = namedtuple(
    'Incoming',
    ['packer', 'unpacker'],
)

class Node(object):
    """Node is a partipant in the cluster.

    Nodes consist of two major pieces of logic: the code that executes
    the call handler, and the call interface. The call interface is
    how Node communicates with other nodes, whereas the call handler
    is customizable logic to be executed when the Node receives a
    request.

    Incoming requests come through _incoming_server socket. Responses
    are sent back along the same connection. Logic that initiates an
    outgoing call to a peer uses the appropriate socket in the
    _outgoing set.

    Node handles periodically attempting to connect to all of its
    peers.

    """

    def __init__(self, loop, port, call_interface, call_handler):
        self._logger = logging.getLogger('wade.chorus')

        self._loop = loop
        self._port = port
        self._call_interface = call_interface
        self._call_handler = call_handler

        self._incoming = {}
        self._incoming_server = pyuv.TCP(self._loop)
        self._incoming_server.bind(('0.0.0.0', self._port))
        self._incoming_server.listen(self._incoming_connection)

    def unwind_loop(self):
        """Shuts down and unregisters everything from the loop."""

        self._incoming_server.close()
        for c in self._incoming.keys():
            c.close()

    def _incoming_connection(self, server, error):
        """Called on a remote client's attempt to connect to this Node."""

        if error is not None:
            return

        client = pyuv.TCP(self._loop)
        server.accept(client)
        self._incoming[client] = Incoming(
            msgpack.Packer(),
            msgpack.Unpacker(),
        )
        client.start_read(self._incoming_read)

    def _incoming_read(self, client, data, error):
        """Received data on incoming socket. Parse it and send it to the
        call_handler.

        """

        if error is not None:
            client.close()
            del self._incoming[client]
            return

        incoming = self._incoming[client]
        incoming.unpacker.feed(data)
        for req_id, message in incoming.unpacker:
            self._call_handler(
                partial(self._queue_response,
                        client, req_id),
                self._call_interface.queue_call,
                message,
            )

    def _queue_response(
            self,
            resp_address,
            req_id,
            status,
            message):
        """Queues a response to an incoming request.

        Call handlers supply the status and message arguments to this
        function. The other arguments are bound by _incoming_read.
        Typically the last thing a call handler does is call:

          resp(chorus.OK, 'my-return-value')

        Where resp is the partially applied version of
        _queue_response.

        """

        incoming = self._incoming.get(resp_address)
        if not incoming:
            return

        data = incoming.packer.pack([req_id, status, message])
        resp_address.write(data)


# Represents an outgoing connection.
Outgoing = namedtuple(
    'Outgoing',
    ['handle', 'packer', 'unpacker', 'remote_addr'],
)

class CallInterface(object):
    """This is the half that makes outgoing connections to chorus
    peers.

    """

    def __init__(self, loop):
        self._logger = logging.getLogger('wade.chorus')

        self._loop = loop
        self._conf = {}
        self._req_counter = 0
        self._callbacks = {} # map req_id -> defn callback(status, msg)
        self._timers = {} # map req_id -> Timer

        self._outgoing = {} # map peer_id -> Outgoing
        self._outgoing_timer = pyuv.Timer(self._loop)
        self._outgoing_timer.start(self._outgoing_check_cb, 0, 1)

    def load_conf(self, conf):
        self._conf = conf

    def unwind_loop(self):
        self._outgoing_timer.stop()

        for outgoing in self._outgoing.values():
            if not outgoing:
                continue
            outgoing.handle.close()

    def queue_call(self, peer_id, message, callback, timeout=None):
        """Queues up a call to a peer.

        Callback will get activated when the peer responds. The
        callback function is called with the response message.

        """

        outgoing = self._outgoing[peer_id]
        req_id = self._req_counter
        self._req_counter += 1
        self._callbacks[req_id] = callback

        timeout_timer = pyuv.Timer(self._loop)
        self._timers[req_id] = timeout_timer
        timeout_timer.start(
            partial(self._call_timeout_cb, peer_id, req_id),
            timeout=timeout,
            repeat=0,
        )

        outgoing.handle.write(outgoing.packer.pack([req_id, message]))

    def _outgoing_check_cb(self, timer):
        """Checks to see if we're maintaining connectivity to all required
        peers. Attempts to re-connect missing connections. Cleans up
        connections we shouldn't have.

        """

        for peer_id, remote in self._conf.items():
            if peer_id in self._outgoing:
                continue

            # A None value means that we're currently attempting a
            # connection.
            self._outgoing[peer_id] = None

            remote_host, remote_port = remote
            self._logger.info(
                "attempting connection to %s:%d",
                remote_host,
                remote_port,
            )

            remote_ip = socket.gethostbyname(remote_host)
            client = pyuv.TCP(self._loop)
            client.connect(
                (remote_ip, remote_port),
                partial(self._outgoing_connect_cb, peer_id),
            )

        for peer_id, outgoing in self._outgoing.items():
            if not outgoing:
                continue

            if outgoing.remote_addr == self._conf.get(peer_id):
                continue

            # if we get here then we have an outgoing connection that
            # doesn't belong
            self._logger.info(
                "closing unncessary connection to %s",
                outgoing.remote_addr,
            )
            self._outgoing_read_cb(
                peer_id,
                outgoing.handle,
                None,
                "force close",
            )

    def _outgoing_connect_cb(self, peer_id, tcp_handle, error):
        """Called on attempt to make outgoing connection to a peer."""

        if error is not None:
            self._logger.error(
                "unable to establish connction to peer %d",
                peer_id,
            )
            del self._outgoing[peer_id]
            return

        self._outgoing[peer_id] = Outgoing(
            tcp_handle,
            msgpack.Packer(),
            msgpack.Unpacker(),
            self._conf[peer_id],
        )

        tcp_handle.start_read(partial(self._outgoing_read_cb, peer_id))
        self._logger.info("connect to peer %d", peer_id)

    def _outgoing_read_cb(self, peer_id, tcp_handle, data, error):
        """Called on receipt of response from a call to a peer."""

        if error is not None:
            tcp_handle.close()
            del self._outgoing[peer_id]
            return

        outgoing = self._outgoing[peer_id]
        outgoing.unpacker.feed(data)
        for payload in outgoing.unpacker:
            req_id, status, message = payload
            callback = self._callbacks.get(req_id)
            timer = self._timers.get(req_id)

            if timer is not None:
                timer.stop()
                del self._timers[req_id]

            if callback is not None:
                del self._callbacks[req_id]
                callback(status, message)

    def _call_timeout_cb(self, peer_id, req_id, timeout_handle):
        """Called at timeout period specified by user into queue_call. This cb
        is always called, even if the request completed.

        """
        timeout_handle.stop()
        callback = self._callbacks.get(req_id)
        if callback is None:
            return
        del self._callbacks[req_id]
        callback(TIMEOUT, None)


class ClientError(Exception):
    pass


class Client(object):
    """Synchronous client implementation.

    This is a piss poor implementation. It's not threadsafe and
    greedily tries to establish connections to peers, and actually
    doesn't seem to handle peer disconnections at all. Failure mode:
    spectacular.

    """

    def __init__(self, conf):
        self._conf = conf
        self._req_count = 0
        self._peers = {}
        self._packer = msgpack.Packer()
        self._unpacker = msgpack.Unpacker()

    def reqrep(self, peer_id, message):
        """Simulates a reqrep call pattern."""

        self._ensure_connection(peer_id)

        data = [self._req_count, message]
        self._req_count += 1
        peer = self._peers[peer_id]
        peer.sendall(self._packer.pack(data))

        while True:
            data = peer.recv(65536)
            if not data:
                raise ClientError("fetch from peer %d failed" % peer_id)

            self._unpacker.feed(data)
            try:
                req_id, status, message = self._unpacker.unpack()
            except msgpack.OutOfData:
                continue

            return status, message

    def _ensure_connection(self, peer_id):
        if peer_id in self._peers:
            return

        sock = socket.create_connection(self._conf[peer_id])
        self._peers[peer_id] = sock
