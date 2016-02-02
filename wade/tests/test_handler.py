#!/usr/bin/env python

import unittest
from collections import defaultdict
from collections import deque
from functools import partial

import mock
import yaml

from wade import chain
from wade import chorus


CONF = yaml.load("""\
version: 1

nodes:
  0: ["localhost", 12340]
  1: ["localhost", 12341]
  2: ["localhost", 12342]
  3: ["localhost", 12343]

chain_map:
  0: [0, 1, 2, 3]
  1: [2, 1, 0]
  2: [2, 3, 1, 0]
  3: [0, 3]
  4: [3, 0]
""")


class Queue(object):
    """A FIFO queue backed by a deque.
    """
    def __init__(self):
        self.queue = deque()

    def __len__(self):
        return len(self.queue)

    def __repr__(self):
        return self.queue.__repr__()

    def enqueue(self, value):
        self.queue.append(value)

    def dequeue(self):
        return self.queue.popleft()

    def peek_front(self):
        return self.queue[0]

    def peek_back(self):
        return self.queue[-1]


class NodeState(object):
    """Encapsulates all state for a given WADE node and simulates 'the loop.'

    A WADE node is composed of a few objects which interact to:

    1) Receive requests from clients or other WADE nodes (Node/Handler).
    2) Make outgoing requests to other nodes (CallInterface).
    3) Query or update data on a node (Store).

    NodeState keeps track of references to all of these objects for a single
    node.

    Additionally, NodeState helps mimic the pyuv event loop. The
    MockCallInterface, instead of writing a message to a peer's socket, adds
    messages to the pending_call queue. Then, when run_loop(...)
    is called the pending events are executed sending messages to the relevant
    peers and providing a way for the peer to respond.
    """
    def __init__(self,
                 node_id,
                 node_states):
        self.node_id = node_id
        self.node_states = node_states # for communication with other nodes.
        self.pending_calls = Queue()
        self.store = DummyStore()
        self.call_interface = MockCallInterface(self)
        self.resp = mock.Mock()

        self.handler = chain.Handler(node_id,
                                     self.store,
                                     self.call_interface,
                                     True)

    def _send_message(self, peer_id, raw_cmd, resp_callback):
        """
        When processing a message that needs to be sent to a peer we enqueue a
        partial function of the peer's handler with the relevant arguments to
        the peer's pending set. The message added to the peer is a 'call'
        message.
        """
        def resp(status, message):
            """
            Allows a peer to respond to another node's request. Note how self
            refers to the current node but the resp fn will be called by the
            peer.
            """
            self.pending_calls.enqueue({
                'method': 'response',
                'fn': partial(resp_callback, status, message),
            })

        peer_state = self.node_states[peer_id]
        call_peer = peer_state.handler._call_interface.queue_call
        peer_state.pending_calls.enqueue({
            'method': 'call',
            'fn': partial(peer_state.handler, resp, call_peer, raw_cmd),
        })

    def handler_with_cmd(self, cmd):
        return self.handler(self.resp, self.call_interface.queue_call, cmd)

    def run_loop(self):
        """
        For a given node, execute the set of pending calls that have been
        added via:

        1) Calling the current node's handler with an update command, queuing
           up a call to peer.
        2) Added by another node's execution of run_loop to simulate messages
           being sent or received among nodes.

        There are 3 message types: send, response, and call.
        1) Send: Added to the pending set by CallInterface.queue_call(...) to
           indicate the given message needs to be sent. A 'call' message is
           added to the peer's pending queue when processing a 'send' message.
        2) Response: This message is enqueued to the current node's pending set
           when the peer responds via the resp function.
        3) Call: Added to a peer when processing a node's 'send' message.
        """

        while self.pending_calls:
            args = self.pending_calls.dequeue()

            method = args.get('method')
            peer_id = args.get('peer_id')
            raw_cmd = args.get('raw_cmd')
            fn = args.get('fn')

            if method == 'send':
                self._send_message(peer_id, raw_cmd, fn)
            elif method in ['response', 'call']:
                fn()
            else:
                raise ValueError('method %s undefined' % (method))

    def set_conf(self, conf):
        """
        To correctly forward messages the handler needs to know the current
        configuration.
        """
        resp = mock.Mock()
        raw_cmd = {'obj_seq': None,
                   'debug_tag': 'reload-conf',
                   'args': {'conf': conf},
                   'obj_id': None,
                   'op': '.RELOAD_CONFIG'}
        self.handler(resp, None, raw_cmd) # special ops don't need a call_peer
        resp.assert_called_with(chorus.OK, 'OK')
        assert(self.handler._conf == conf)

    @staticmethod
    def run_loops(node_states):
        """
        Run node_states loops until there are no pending calls on any nodes.

        @param node_states, dict(node_id -> NodeState)
        """
        while True:
            for node_state in node_states.itervalues():
                node_state.run_loop()

            pending = any(len(node_state.pending_calls) > 0
                          for node_state in node_states.itervalues())

            if not pending:
                break


class DummyStore(chain.Store):
    """Stores a single value for each obj_id.
    """
    def __init__(self):
        self._data = {}
        self._seq_map = defaultdict(int)

    def serialize_obj(self, obj_id):
        raise NotImplementedError()

    def deserialize_obj(self, obj_id, value):
        raise NotImplementedError()

    def max_seq(self, obj_id):
        return self._seq_map[obj_id]

    @chain.update_op
    def SET(self, obj_id, obj_seq, args):
        self._data[obj_id] = args['v']
        self._seq_map[obj_id] = obj_seq
        return 'OK'

    @chain.query_op
    def GET(self, obj_id, obj_seq, args):
        return self._data[obj_id] 


class MockCallInterface(object):
    """
    Simulates the CallInterface with public methods queue_call
    and load_conf. Instead of immediately writing to the peer's outgoing
    socket we add the given message to the current node's pending set.
    """
    def __init__(self, node_state):
        self.node_state = node_state

    def load_conf(self, conf): pass

    def queue_call(self, peer_id, message, callback, timeout=None):
        """
        Enqueue a message to be forwarded to peer_id when
        self.node_state.run_loop() is executed. Timeout not simulated.
        """
        self.node_state.pending_calls.enqueue({'method': 'send',
                                               'peer_id': peer_id,
                                               'raw_cmd': message,
                                               'fn': callback})


def create_get_cmd(obj_id):
    return {'obj_seq': -1,
            'debug_tag': 'test-kv',
            'args': {},
            'obj_id': obj_id,
            'op': 'GET'}


def create_set_cmd(obj_id, value, obj_seq=-1):
    return {'obj_seq': obj_seq,
            'debug_tag': 'test-kv',
            'args': {'v': value},
            'obj_id': obj_id,
            'op': 'SET'}


class HandlerTest(unittest.TestCase):

    def setUp(self):
        self.conf = CONF
        self.node_states = {} # map node_id -> NodeState

        # Setup each node state and load the conf.
        for node_id in self.conf['nodes'].keys():
            node_state = NodeState(node_id, self.node_states)
            node_state.set_conf(self.conf)
            self.node_states[node_id] = node_state

    def test_simple_read_write(self):
        for obj_id, chain in self.conf['chain_map'].items():
            head_id, tail_id, body_ids = chain[0], chain[-1], chain[1:-1]
            head_state = self.node_states[head_id]
            tail_state = self.node_states[tail_id]

            # Send update commands to the head
            head_state.handler_with_cmd(create_set_cmd(obj_id, 'foo'))
            head_state.handler_with_cmd(create_set_cmd(obj_id, 'bar'))

            # Execute loops on all nodes in the chain.
            NodeState.run_loops(self.node_states)

            # Check head responded successfully.
            head_state.resp.assert_called_with(chorus.OK, 'OK')

            # Check that we can query data and that the last write won.
            tail_state.handler_with_cmd(create_get_cmd(obj_id))
            tail_state.resp.assert_called_with(chorus.OK, 'bar')

            # Add another update but don't execute loops
            head_state.handler_with_cmd(create_set_cmd(obj_id, 'america'))

            # Check that tail hasn't set 'america' value.
            tail_state.handler_with_cmd(create_get_cmd(obj_id))
            tail_state.resp.assert_called_with(chorus.OK, 'bar')

            # Run loops and check 'america' was set
            NodeState.run_loops(self.node_states)
            tail_state.handler_with_cmd(create_get_cmd(obj_id))
            tail_state.resp.assert_called_with(chorus.OK, 'america')

            self.setUp()
