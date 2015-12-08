"""Chain replication and command interface logic.

"""

import random
import logging
import traceback
from functools import partial
from collections import namedtuple

import chorus


# Command represents the call format. Clients create dictionaries that
# contain all of the Command fields. One tricky note: originating
# commands have obj_seq set to -1. The head of the chain is
# responsible for generating a new obj_seq and rewriting the command
# it forwards.
Command = namedtuple('Command',
                     [
                         'obj_id',    # hashable type, object id
                         'obj_seq',   # int, sequence for this object
                         'op',        # str, operation for this command
                         'args',      # anything, arguments for op
                         'debug_tag', # prefix string for debug output
                         'internal',  # True for client requests
                     ])


class CallState(object):
    """Represents an attempt to send an update command down the chain. If
    the call is successful and committed, CallState will call the
    supplied OK continuation (ok_cont). Otherwise it uses the fail
    continuation (fail_cont).

    """

    def __init__(
            self,
            my_id,
            ok_cont,
            fail_cont,
            call_peer,
            cmd,
            chain,
            is_head,
            is_tail,
            is_body,
            store,
            logger,
    ):
        self._my_id = my_id
        self._ok_cont = ok_cont
        self._fail_cont = fail_cont
        self._call_peer = call_peer
        self._cmd = cmd

        self._func = store.get_op(self._cmd.op)
        self._max_seq = store.max_seq
        self._chain = chain
        self._is_head = is_head
        self._is_tail = is_tail
        self._is_body = is_body

        self._next_node_id = None

        self._logger = logger

    def _cmd_prepare(self):
        """Decide whether or not command needs to be forwarded to a
        successor.

        """

        if self._cmd.internal:
            should_forward = not self._is_tail
            if should_forward:
                chain_pos = self._chain.index(self._my_id)
                self._next_node_id = self._chain[chain_pos + 1]
                next_cmd = self._cmd._asdict()
        else:
            should_forward = True
            if self._func._op_type == UPDATE_OP:
                self._next_node_id = self._chain[0]
            elif self._func._op_type == QUERY_OP:
                self._next_node_id = self._chain[-1]
            next_cmd = self._cmd._asdict()
            next_cmd['internal'] = True

        if should_forward:
            self._call_peer(
                self._next_node_id,
                next_cmd,
                self._cmd_commit,
                timeout=1.0,
            )
        else:
            self._cmd_commit(chorus.OK, None)

    def _cmd_commit(self, status, message):
        if status != chorus.OK:
            self._fail_cont('%d forward to %d failed: %s' % \
                            (self._my_id, self._next_node_id, message))
            return

        if not self._cmd.internal:
            self._ok_cont(message)
            return

        if self._func._op_type == UPDATE_OP:
            cur_seq = self._max_seq(self._cmd.obj_id)
            if self._cmd.obj_seq != cur_seq + 1:
                self._fail_cont(
                    '%d out of order commit attempt, cur = %d, attempt = %d' % \
                    (self._my_id, cur_seq, self._cmd.obj_seq))
                return

        try:
            ret = self._func(
                self._cmd.obj_id,
                self._cmd.obj_seq,
                self._cmd.args,
            )
            self._ok_cont(ret)
            return
        except Exception:
            self._fail_cont(traceback.format_exc())
            # Hm, maybe re-raising is a better idea?
            return


class Handler(object):
    """Call hander.

    Sets up a CallState and passes on control. Also takes care of the
    pending set.

    """

    def __init__(self, my_id, store, call_interface):
        self._logger = logging.getLogger('wade.chain')

        self._my_id = my_id
        self._store = store
        self._call_interface = call_interface

        self._max_seq = store.max_seq
        self._seq_map = {}
        self._pending = {}
        self._chain_map = {}
        self._conf = {}

        # special ops tell the node to do something, such as stop
        # reload config, stop accepting requests,but aren't regular
        # object commands
        self._special_ops = {
            '.RELOAD_CONF': ReloadConfOp(self, call_interface, self._logger),
        }

    def __call__(self, resp, call_peer, raw_cmd):
        fail = partial(resp, chorus.ERR)

        try:
            cmd = Command(**raw_cmd)
        except TypeError:
            fail('invalid command: %s' % raw_cmd)
            return

        special_func = self._special_ops.get(cmd.op)
        if special_func:
            special_func(cmd, resp)
            return

        func = self._store.get_op(cmd.op)
        if not func:
            fail('no function defined for op: %s' % cmd.op)
            return

        chain = self._chain_map.get(cmd.obj_id)
        if not chain:
            fail('no chain for obj %s' % cmd.obj_id)
            return

        in_chain = self._my_id in chain
        is_head = chain[0] == self._my_id
        is_tail = chain[-1] == self._my_id
        is_body = in_chain and not (is_head or is_tail)
        obj_seq_assigned = cmd.obj_seq != -1

        # Optimization: We can convert an external update at the head
        # to internal. Similarly, we can convert an external query at
        # the tail to internal.
        if not cmd.internal:
            if (func._op_type == UPDATE_OP and is_head) or \
               (func._op_type == QUERY_OP and is_tail):
                cmd = cmd._replace(internal=True)

        if cmd.internal:
            if not in_chain:
                fail('misplaced internal forward, obj_id %s, chain: %s' % \
                     (cmd.obj_id, chain))
                return

            if func._op_type == UPDATE_OP:
                # If we're the head and this is a brand new update
                # command, we haven't yet set the obj_seq. If we're
                # not at the head then we expect the obj_seq to have
                # been set.
                if is_head and obj_seq_assigned:
                    fail('update at head already assigned seq, obj_id %s, chain: %s' % \
                         (cmd.obj_id, chain))
                    return
                if not is_head and not obj_seq_assigned:
                    fail('update command not at head, obj_id %s, chain: %s' % \
                         (cmd.obj_id, chain))
                    return

                # So to recap the logic from the above two if
                # statements. Once we get here, we are either: the
                # head and there is no assigned obj seq OR *not* the
                # head and there *is* an assigned obj seq.

                if is_head:
                    cmd = self._add_obj_seq(cmd, chain)

            if func._op_type == QUERY_OP and not is_tail:
                fail('query command not at tail, obj_id %s, chain: %s' % \
                     (cmd.obj_id, chain))
                return

            # fixme: need to also reject commands with sequences
            # less than the max in the pending set
            if (cmd.obj_id, cmd.obj_seq) in self._pending:
                fail('request %d:%d already exists' % (cmd.obj_id, cmd.obj_seq))
                return

            self._pending[(cmd.obj_id, cmd.obj_seq)] = cmd

        if not cmd.internal:
            placement = 'FORWARD'
        elif is_head:
            placement = 'HEAD'
        elif is_tail:
            placement = 'TAIL'
        elif is_body:
            placement = 'BODY'
        else:
            # Should never happen. The 'cmd.internal and not in_chain'
            # check above should make this a logical impossibility,
            # unless something else is fubar'd.
            placement = 'ERROR'
        self._logger.debug("CMD %s %s", placement, cmd)

        state = CallState(
            self._my_id,
            partial(self._handle_ok, resp, cmd),
            partial(self._handle_fail, resp, cmd),
            call_peer,
            cmd,
            chain,
            is_head,
            is_tail,
            is_body,
            self._store,
            self._logger,
        )
        state._cmd_prepare()

    def _add_obj_seq(self, cmd, chain):
        """Returns a Command with an obj_seq if this is the head."""

        max_seq = self._seq_map.get(cmd.obj_id) or self._max_seq(cmd.obj_id)
        obj_seq = max_seq + 1
        self._seq_map[cmd.obj_id] = obj_seq
        cmd = cmd._replace(obj_seq=obj_seq)

        return cmd

    def _handle_ok(self, resp, cmd, ret):
        self._pending.pop((cmd.obj_id, cmd.obj_seq), None)
        resp(chorus.OK, ret)

    def _handle_fail(self, resp, cmd, error):
        debug_msg = '%s -- %s' % (cmd.debug_tag, error)
        self._logger.error(debug_msg)
        resp(chorus.ERR, error)

    def set_conf(self, conf):
        self._conf = conf
        self._chain_map = self._conf['chain_map']


class ReloadConfOp(object):
    def __init__(self, handler, call_interface, logger):
        self._handler = handler
        self._call_interface = call_interface
        self._logger = logger

    def __call__(self, cmd, resp):
        conf = cmd.args.get('conf')
        if not conf:
            self._logger.error('error loading config')
            resp(chorus.ERR, 'unable to set empty config')
            return

        self._logger.warn('loading config version %s' % conf['version'])
        self._call_interface.load_conf(conf['nodes'])
        self._handler.set_conf(conf)
        resp(chorus.OK, 'OK')


UPDATE_OP = 1
QUERY_OP = 2

def update_op(func):
    """Update (mutating) op."""

    func._op_type = UPDATE_OP
    func._op_name = func.__name__

    return func

def query_op(func):
    """Query (non-mutating) op."""

    func._op_type = QUERY_OP
    func._op_name = func.__name__

    return func

class Store(object):
    """Abstract class for a store of object states.

    In addition to the NotImplemented functions listed,
    implementations need to define update and query operators using
    the @update_op and @query_op decorators.

    Example use:

    class KVStore(chain.Store):
        def __init__(self):
            # my init

        @chain.update_op
        def SET(self, obj_id, obj_seq, args):
            # ...

        @chain.query_op
        def GET(self, obj_id, obj_seq, args):
            # ...

    """

    # fixme: rename these to parceling
    def serialize_obj(self, obj_id):
        raise NotImplementedError()

    def deserialize_obj(self, obj_id, value):
        raise NotImplementedError()

    def max_seq(self, obj_id):
        raise NotImplementedError()

    def get_op(self, op_name):
        func = getattr(self, op_name)
        if hasattr(func, '_op_type'):
            return func
        else:
            return None


class Client(object):
    """Chain client.

    Use to communicate with chain nodes.

    """

    def __init__(self, conf):
        self._conf = conf
        self._chorus_client = chorus.Client(self._conf['nodes'])
        self._peer_ids = self._conf['nodes'].keys()

    def call(self, op_name, obj_id, args, debug_tag):
        """Sends command to cluster."""

        peer_id = self._conf['chain_map'][obj_id][0]
        return self._chorus_client.reqrep(
            peer_id,
            {
                'obj_id': obj_id,
                'obj_seq': -1,
                'op': op_name,
                'args': args,
                'debug_tag': debug_tag,
                'internal': False,
            },
        )

    def special_op(self, peer_id, op_name, args, tag):
        """Sends a special op to the peer, or to all peers if peer_id is
        None.

        """

        if peer_id is None:
            peer_ids = self._peer_ids
        else:
            peer_ids = [peer_id]

        return { peer_id:
                 self._chorus_client.reqrep(
                     peer_id,
                     {
                         'obj_id': None,
                         'obj_seq': None,
                         'op': op_name,
                         'args': args,
                         'debug_tag': tag,
                         'internal': False,
                     })
                 for peer_id in peer_ids }
