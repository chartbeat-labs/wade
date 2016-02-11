
"""An peer communcation + event loop implementation which allows for
single threaded asynchronous communication amongst a set of equal
peers.

"""

import logging
import socket
import threading
from functools import partial
from collections import namedtuple
from select import select


import pyuv
import msgpack
from posix_ipc import BusyError
from posix_ipc import Semaphore
from posix_ipc import O_CREX

from circular_buffer import CircularBuffer
from circular_buffer import CircularBufferError


OK = 'ok'
ERR = 'err'
CALL_LIMIT = 'call_limit'
PEER_DISCONNECT = 'peer_disconnect'
TIMEOUT = 'timeout'
RECV_BYTES = 1024 * 64
SEND_BYTES = 1024 * 64
OUTGOING_PEER_BUFFER_SIZE = 1024 ** 2 # 1 MB, controls max request size
SOCKET_CREATION_TIMEOUT = 1
SELECT_TIMEOUT = 1 # timeout for select.select


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
        resp_address.write(data)

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


""" Begin client code """


class ClientError(Exception):
    pass


class Peer(object):
    def __init__(self, peer_id, socket):
        self.peer_id = peer_id
        self.socket = socket
        self.pending_requests = {} # req_id -> ValueEvent
        self.incoming_buffer = msgpack.Unpacker()
        self.outgoing_buffer = CircularBuffer(OUTGOING_PEER_BUFFER_SIZE)
        self.lock = threading.Lock()

    def close(self):
        try:
            self.lock.release()
        except threading.ThreadError: 
            pass # lock may not have been acquired. 
        self.socket.close()

    def add_request(self, req_id, request_bytes, value_event):
        """Add ValueEvent to pending requests and write request_bytes to
        outgoing_buffer"""
        with self.lock:
            self.pending_requests[req_id] = value_event
            self.outgoing_buffer.write(request_bytes)

    def handle_response(self, response):
        """Extract req_id from response and check if there is a pending request 
        for the req_id. If so set the value.
        """
        with self.lock:
            req_id, status, message = response
            if req_id in self.pending_requests: # request may have timed out
                self.pending_requests[req_id].set_value((status, message))

    def remove_request(self, req_id):
        with self.lock:
            del self.pending_requests[req_id]


class ValueEventTimeout(Exception):
    def __init__(self, timeout):
        super(ValueEventTimeout, self).__init__(
            'Attempt to wait for ValueEvent timed out after %s s' % timeout
        )


class ValueEvent(object):
    """Provides behavior similar to threading.Event. However, this
    allows associating a value when "setting" (or notifying) the Event object.

    The ValueEvent object is used to communicate between two threads when one
    thread needs a value that is computed by another. The reading thread waits
    on the value (but does not set it), and the writing thread sets the value
    then triggers the event. In other words, there is exactly one write to the
    value from one thread, and one read from a different thread, and the
    ValueEvent object guarantees they don't overlap so long as the
    wait_for_value and set_value methods are used by the reading and writing
    thread, respectively.

    Additionally, Python's threading.Event object provides a timeout feature
    like ours but introduces unacceptable delays. For that reason we use
    posix_ipc.Semaphore to mimic threading.Event without the performance
    penalty.

    http://stackoverflow.com/questions/21779183/python-eventwait-with-timeout-gives-delay
    """
    def __init__(self, name=None):
        self._value = None
        self._semaphore = Semaphore(name, flags=O_CREX)

    def __del__(self):
        # If unlink isn't explicitly called the OS will *not* release the
        # semaphore object, even if the program crashes. We may want to spawn
        # a new process to manage them or give the semaphores known names when
        # creating them so they can be reclaimed on restart.
        self._semaphore.unlink()

    def wait_for_value(self, timeout):
        try:
            self._semaphore.acquire(timeout)
        except BusyError:
            raise ValueEventTimeout(timeout)
        return self._value

    def set_value(self, value):
        self._value = value
        self._semaphore.release()


class Client(object):
    """Multithreaded client implementation.
    """

    def __init__(self, conf, timeout):
        self._conf = conf
        self._timeout = timeout
        self._unpacker = msgpack.Unpacker()
        self._packer = msgpack.Packer()

        # connection variables
        self._peers = {} # peer_id -> Peer
        self._sock_to_peer = {} # socket.connection -> Peer
        self._peers_lock = threading.Lock() # for _peers and _sock_to_peers

        # request / response variables
        self._req_count = 0
        self._req_count_lock = threading.Lock()

        # For reuse of ValueEvent objects by a thread.
        self._threadlocal = threading.local()
        
        self._bg_thread = threading.Thread(
            target=self._process_requests_in_background
        )
        self._bg_thread.setDaemon(True)
        self._bg_thread.start()

    def _bg_clean_up_peer(self, peer):
        """Remove peer and associated sockets from _peers and _sock_to_peer.
        Also close the peer.
        """
        with self._peers_lock:
            del self._peers[peer.peer_id]
            del self._sock_to_peer[peer.socket]
        peer.close()
        peer = None

    def _bg_select_peers(self, timeout=SELECT_TIMEOUT):
        """Similar to select.select, but instead of returning sockets this
        returns the associated Peer objects.
        """
        with self._peers_lock:
            socks = self._sock_to_peer.keys()
        
        if not socks:
            return [], [], []

        readable, writable, exceptional = select(socks, socks, socks, timeout)
        with self._peers_lock:
            readable = [self._sock_to_peer[s] for s in readable]
            writable = [self._sock_to_peer[s] for s in writable]
            exceptional = [self._sock_to_peer[s] for s in exceptional]
        return readable, writable, exceptional

    def _process_requests_in_background(self):
        """Executes forever, handles sending and receiving messages from peers.
        Meant to be called in a background thread (see constructor).

        This checks for peers with readable data (added by reqrep) and feeds the
        data from its socket into the peer's incoming_buffer until a message can
        be read. The request id is read from the message and if a pending
        request exists the associated ValueEvent object is "set" with the
        message contents.

        For each writable peer, this function reads data from the peer's
        outgoing buffer, if it's not empty, and sends it along on the peer's
        socket.

        This loop also handles client disconnects / errors.
        """
        while True:
            readable, writable, exceptional = self._bg_select_peers()

            for peer in readable:
                data = peer.socket.recv(RECV_BYTES)
                if data:
                    peer.incoming_buffer.feed(data)
                    try:
                        response = peer.incoming_buffer.unpack()
                    except msgpack.OutOfData:
                        continue
                    peer.handle_response(response)
                else:
                    self._bg_clean_up_peer(peer)
                    if peer in writable:
                        writable.remove(peer)
                    if peer in exceptional:
                        exceptional.remove(peer)

            for peer in writable:
                # single-reader configuration means we can safely unlock between
                # peeking and committing.
                with peer.lock:
                    next_bytes = peer.outgoing_buffer.peek(SEND_BYTES)
                if not next_bytes:
                    continue

                sent_bytes = peer.socket.send(next_bytes)
                if sent_bytes == 0:
                    self._bg_clean_up_peer(peer)
                    if peer in exceptional:
                        exceptional.remove(peer)
                    continue

                with peer.lock:
                    peer.outgoing_buffer.commit_read(sent_bytes)

            for peer in exceptional:
                print 'handling exceptional condition for peer %s at %s' % \
                          (peer.peer_id, peer.socket.getpeername())
                self._bg_clean_up_peer(peer)

    def _ensure_value_event(self):
        """A ValueEvent object is used by one thread at a time so it can be
        safely reused instead of creating a new one for each call to reqrep.
        This avoids the overhead associated with creating ValueEvent objects,
        which encapsulate OS semaphores, by instantiating only one for each
        calling thread.

        @return: ValueEvent
        """
        ev = getattr(self._threadlocal, 'event', None)
        if ev is None:
            ev = ValueEvent()
            self._threadlocal.event = ev
        return ev

    def _ensure_connection(self, peer_id, timeout=SOCKET_CREATION_TIMEOUT):
        """Connects to the peer's socket and creates a Peer object.

        @return: Peer
        """
        with self._peers_lock:
            if peer_id not in self._peers:
                sock = socket.create_connection(self._conf[peer_id],
                                                timeout=timeout)
                peer = Peer(peer_id, sock)
                self._peers[peer_id] = peer
                self._sock_to_peer[sock] = peer

            return self._peers[peer_id]

    def _inc_req_count(self):
        with self._req_count_lock:
            curr = self._req_count
            self._req_count += 1
            return curr

    def close(self):
        for peer in self._peers.values():
            self._bg_clean_up_peer(peer)

    def reqrep(self, peer_id, message, timeout=False):
        """The message is serialized and added to the peer's outgoing buffer
        with an associated request id (add_request). The request id is also used
        to associate a ValueEvent object with the call in the peer's
        pending_requests dict such that the background thread,
        _process_requests_in_background, can "respond" to the reqrep call by
        setting the ValueEvent object.

        @param timeout, float. In seconds, optional if set on client
            initialization. Specify None for no timeout.
        """
        if timeout is False and self._timeout is False:
            raise ClientError('must specify timeout value')
        elif timeout is False:
            timeout = self._timeout

        req_id = self._inc_req_count()
        peer = self._ensure_connection(peer_id)
        value_event = self._ensure_value_event()
        outgoing_bytes = self._packer.pack([req_id, message])

        peer.add_request(req_id, outgoing_bytes, value_event)
        try:
            status, message = value_event.wait_for_value(timeout)
        except ValueEventTimeout:
            status = TIMEOUT
            message = 'request timed out after %s s' % (timeout)
        peer.remove_request(req_id)
        return status, message
