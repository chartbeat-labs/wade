
"""An peer communcation + event loop implementation which allows for
single threaded asynchronous communication amongst a set of equal
peers.

"""

import logging
import socket
import threading
import Queue
from functools import partial
from collections import defaultdict
from collections import namedtuple
from select import select


import pyuv
import msgpack
from posix_ipc import Semaphore, O_CREX, BusyError


OK = 'ok'
ERR = 'err'
CALL_LIMIT = 'call_limit'
PEER_DISCONNECT = 'peer_disconnect'
TIMEOUT = 'timeout'
RECV_BYTES = 65536
SOCKET_CREATION_TIMEOUT = 1
SELECT_TIMEOUT = 1000 # timeout for select.select


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


class ClientError(Exception):
    pass


""" Begin client code """


class Peer(object):
    def __init__(self, peer_id, socket):
        self.peer_id = peer_id
        self.socket = socket
        self.outgoing_queue = Queue.Queue()
        self.incoming_buffer = msgpack.Unpacker()
        self.lock = threading.Lock()

    def close(self):
        try:
            self.lock.release()
        except threading.ThreadError: 
            pass # lock may not have been acquired. 
        self.socket.close()


class ValueEvent(object):
    """This class provides behavior similar to threading.Event. However, it
    allows associating a value when "setting" (or notifying) the Event object.

    Additionally, threading.Event is substituted by posix_ipc.Semaphore
    to avoid the delay when waiting for Events with a timeout:
    http://stackoverflow.com/questions/21779183/python-eventwait-with-timeout-gives-delay
    """
    def __init__(self, default_value=None):
        '''
        @param default_value, object to return if wait_for_value(...) times out.
        '''
        self.value = default_value
        self.event = Semaphore(None, flags=O_CREX)

    def __del__(self):
        # If unlink isn't explicitly called the OS will *not* release the
        # semaphore object, even if the program crashes. Some applications make
        # use of a separate process whose sole responsibility is to create and
        # destroy semaphores. We may want do this or pass the semaphores known
        # names on initialization so they can be reclaimed on restart.
        self.event.unlink()

    def wait_for_value(self, timeout):
        try:
            self.event.acquire(timeout)
        except BusyError: # timeout
            pass
        return self.value

    def set_value(self, value):
        self.value = value
        self.event.release()

    def reset(self, default_value):
        assert(self.event.value == 0)
        self.value = default_value


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
        self._pending_requests = {} # req_id -> ValueEvent
        self._pending_requests_lock = threading.Lock()        

        # For reuse of ValueEvent objects by a thread.
        self._threadlocal = threading.local()
        
        bg_thread = threading.Thread(target=self._process_requests_in_background)
        bg_thread.setDaemon(True)
        bg_thread.start()

    def _bg_clean_up_peer(self, peer):
        """Remove peer and associated sockets from _peers and _sock_to_peer.
        Also close the peer.
        """
        with self._peers_lock:
            del self._peers[peer.peer_id]
            del self._sock_to_peer[peer.socket]
        peer.close()
        peer = None

    def _bg_handle_response(self, response):
        """Extract req_id from response and check if there is a pending request 
        for the req_id. If so set the value.
        """
        with self._pending_requests_lock:
            req_id, status, message = response
            if req_id in self._pending_requests: # request may have timed out
                self._pending_requests[req_id].set_value((status, message))

    def _bg_select_peers(self, timeout=SELECT_TIMEOUT):
        """Similar to select.select, but instead of returning sockets this
        returns the associated Peer objects.
        """
        with self._peers_lock:
            peers = self._sock_to_peer.keys()
        
        if not peers:
            return [], [], []

        readable, writable, exceptional = select(peers, peers, peers, timeout)
        readable = [self._sock_to_peer[s] for s in readable]
        writable = [self._sock_to_peer[s] for s in writable]
        exceptional = [self._sock_to_peer[s] for s in exceptional]
        return readable, writable, exceptional

    def _process_requests_in_background(self):
        """Executes forever, handles sending and receiving messages from peers.
        Meant to be called in a background thread (see constructor).

        When reqrep is called a message is enqueued to the outgoing requests
        queue of the destination peer with an associated request id. The request
        id is also used to create a ValueEvent object in the _pending_requests
        dict such that this function can "respond" to the reqrep call.

        For each writable peer, this function dequeues a messages from the peers
        outgoing queue, if one exists, and sends it along on the peer's socket.

        Additionally, this function checks for peers with readable data and
        feeds the data into the peer's incoming_buffer until a message can be
        read. The request id is read from the message and if a pending request
        exists the ValueEvent object is "set" with the message contents.

        Additionally, this loop handles client disconnects / errors.
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
                    self._bg_handle_response(response)
                else:
                    self._bg_clean_up_peer(peer)
                    if peer in writable:
                        writable.remove(peer)
                    if peer in exceptional:
                        exceptional.remove(peer)

            for peer in writable:
                try:
                    next_msg = peer.outgoing_queue.get_nowait()
                except Queue.Empty:
                    continue
                peer.socket.sendall(self._packer.pack(next_msg))

            for peer in exceptional:
                print 'handling exceptional condition for peer %s at %s' % \
                          (peer.peer_id, peer.socket.getpeername())
                self._bg_clean_up_peer(peer)

    def _ensure_value_event(self, default_value):
        """This ensures the calling thread has its own ValueEvent object and if
        not creates one and stores it in a thread local. There is significant
        overhead to creating ValueEvent objects since they map to OS semaphores.
        A ValueEvent object is used by one thread at a time so it can be safely
        reused instead of creating a new one for each call to reqrep.

        @param default_value: The default or timeout value for the ValueEvent
            object, either on its construction or if it's being reused.
        @return: ValueEvent
        """
        if not hasattr(self._threadlocal, 'event'):
            value_event = ValueEvent(default_value)
            self._threadlocal.event = value_event
        else:
            value_event = self._threadlocal.event
            value_event.reset(default_value)

        return value_event

    def _ensure_connection(self, peer_id, timeout=SOCKET_CREATION_TIMEOUT):
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
        """
        @param timeout, float. In seconds, optional if set on client
            initialization. Specify None for no timeout.
        """
        if timeout is False and self._timeout is False:
            raise ClientError('must specify timeout value')
        elif timeout is False:
            timeout = self._timeout

        peer = self._ensure_connection(peer_id)      
        req_id = self._inc_req_count()

        timeout_resp = (ERR, 'request timed out after %s s' % (timeout))
        value_event = self._ensure_value_event(timeout_resp)

        with self._pending_requests_lock:
            self._pending_requests[req_id] = value_event

        with peer.lock:
            peer.outgoing_queue.put([req_id, message]) # FIXME put bytes instead!
            value_event.wait_for_value(timeout)
        
        with self._pending_requests_lock:
            status, message = self._pending_requests[req_id].value
            del self._pending_requests[req_id]

        return status, message
