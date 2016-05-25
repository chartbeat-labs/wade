
"""An peer communcation + event loop implementation which allows for
single threaded asynchronous communication amongst a set of equal
peers.

"""

import logging
import socket
import threading
import Queue
from functools import partial
from collections import namedtuple
from contextlib import contextmanager

import pyuv
import msgpack


OK = 'ok'
ERR = 'err'
CALL_LIMIT = 'call_limit'
PEER_DISCONNECT = 'peer_disconnect'
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

        self._periodics = []
        self._setup_periodics()

        self._incoming = {}
        self._incoming_server = pyuv.TCP(self._loop)
        self._incoming_server.bind(('0.0.0.0', self._port))
        self._incoming_server.listen(self._incoming_connection)

    def unwind_loop(self):
        """Shuts down and unregisters everything from the loop."""

        self._incoming_server.close()
        self._unwind_periodics()
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
        try:
            resp_address.write(data)
        except pyuv.error.HandleClosedError:
            # This can happen if the client prematurely closes the
            # connection. Though the better thing to do would be to
            # detect this situation and call a call handler function
            # to take some action.
            pass

    def _setup_periodics(self):
        for period, C in self._call_handler.get_periodics():
            timer = pyuv.Timer(self._loop)
            self._periodics.append(timer)

            # pyuv timers expect an arg, but we don't require that of
            # chorus periodics
            actual = lambda timer: C()
            timer.start(actual, 0, period)

    def _unwind_periodics(self):
        for timer in self._periodics:
            timer.stop()
            del timer

        self._periodics = []


# Represents an outgoing connection.
Outgoing = namedtuple(
    'Outgoing',
    ['handle', 'packer', 'unpacker', 'remote_addr', 'callbacks'],
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

        self._outgoing = {} # map peer_id -> Outgoing
        self._outgoing_timer = pyuv.Timer(self._loop)
        self._outgoing_timer.start(self._outgoing_check_cb, 0, 1)

        self._seemingly_arbitrary_max_request_limit = 10000

    def load_conf(self, conf):
        self._conf = conf

    def unwind_loop(self):
        self._outgoing_timer.stop()

        for outgoing in self._outgoing.values():
            if not outgoing:
                continue
            self._unwind_outgoing(outgoing)

    def queue_call(self, peer_id, message, callback):
        """Queues up a call to a peer.

        Callback will get activated when the peer responds. The
        callback function is called with the response message.

        """

        outgoing = self._outgoing[peer_id]

        in_flight = len(outgoing.callbacks)
        if in_flight >= self._seemingly_arbitrary_max_request_limit:
            callback(
                CALL_LIMIT,
                'can not exceed %d simultaneous call requests' % in_flight,
            )
            return

        req_id = self._req_counter
        self._req_counter += 1
        outgoing.callbacks[req_id] = callback

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
            {},
        )

        tcp_handle.start_read(partial(self._outgoing_read_cb, peer_id))
        self._logger.info("connect to peer %d", peer_id)

    def _outgoing_read_cb(self, peer_id, tcp_handle, data, error):
        """Called on receipt of response from a call to a peer."""

        outgoing = self._outgoing[peer_id]

        if error is not None:
            callbacks = outgoing.callbacks.values()
            self._unwind_outgoing(outgoing)
            del self._outgoing[peer_id]

            for cb in callbacks:
                cb(PEER_DISCONNECT, None)
            return

        outgoing.unpacker.feed(data)
        for payload in outgoing.unpacker:
            req_id, status, message = payload
            callback = outgoing.callbacks.get(req_id)

            if callback is not None:
                del outgoing.callbacks[req_id]
                callback(status, message)

    def _unwind_outgoing(self, outgoing):
        outgoing.handle.close()



class Client(object):
    def __init__(self, conf, per_peer_pool_size=4):
        self._conf = conf
        self._recv_bytes = 64 * 1024
        self._per_peer_pool_size = per_peer_pool_size
        self._next_req_id = 1
        self._next_req_id_lock = threading.Lock()

        self._sockets = {}
        for peer_id in self._conf.keys():
            self._sockets[peer_id] = Queue.Queue(self._per_peer_pool_size)
            for i in xrange(self._per_peer_pool_size):
                self._sockets[peer_id].put([None])

    @contextmanager
    def _get_peer_connection(self, peer_id):
        # fixme: handle timeout
        entry = self._sockets[peer_id].get(block=True)
        sock = entry[0]
        if sock is None:
            sock = socket.create_connection(self._conf[peer_id])
            entry[0] = sock

        try:
            yield sock
        except socket.error as e:
            logging.warning(
                "disconnect from peer %d due to socket error: %s",
                peer_id,
                e,
            )
            del sock
            entry[0] = None

        self._sockets[peer_id].put(entry)

    def reqrep(self, peer_id, message):
        """Grabs a connection from the pool and makes a synchronous
        request/reply call.

        """

        with self._next_req_id_lock:
            req_id = self._next_req_id
            self._next_req_id += 1

        with self._get_peer_connection(peer_id) as sock:
            # We'll let socket errors propagate up so that we can get
            # rid of it from the pool.

            packer = msgpack.Packer()
            sock.sendall(packer.pack([req_id, message]))

            unpacker = msgpack.Unpacker()
            while True:
                data = sock.recv(self._recv_bytes)
                unpacker.feed(data)
                for payload in unpacker:
                    req_id, status, message = payload
                    return status, message

    def close(self):
        pass
