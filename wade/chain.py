"""Chain replication and command interface logic.

"""

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
                         'obj_id',    # int, object id
                         'obj_seq',   # int, sequence for this object
                         'op',        # str, operation for this command
                         'args',      # anything, arguments for op
                         'debug_tag', # prefix string for debug output
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

        self._next_node_id = None

        self._logger = logger

    def _cmd_prepare(self):
        """Decide whether or not command needs to be forwarded to a successor."""

        if self._my_id not in self._chain:
            self._fail_cont(
                'node %d not in chain %s' % (self._my_id, self._chain)
            )
            return

        is_tail = self._chain[-1] == self._my_id

        if not is_tail and self._func._op_type == 'query':
            self._fail_cont(
                'received query command at non-tail node.'
            )
            return

        if is_tail:
            self._cmd_commit(chorus.OK, None)
        else:
            chain_pos = self._chain.index(self._my_id)
            self._next_node_id = self._chain[chain_pos + 1]
            self._call_peer(
                self._next_node_id,
                self._cmd._asdict(),
                self._cmd_commit,
                timeout=1.0,
            )

    def _cmd_commit(self, status, message):
        if status != chorus.OK:
            self._fail_cont('%d forward to %d failed: %s' % \
                            (self._my_id, self._next_node_id, message))
            return

        if self._func._op_type == 'update':
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

    def __call__(self, resp, fail, call_peer, raw_cmd):
        try:
            cmd = Command(**raw_cmd)
        except TypeError:
            fail('invalid command: %s' % raw_cmd)
            return

        special_op = self._special_ops.get(cmd.op)
        if special_op:
            special_op(cmd, resp, fail)
            return
        else:
            chain = self._chain_map.get(cmd.obj_id)
            if not chain:
                fail('no chain for obj %s' % cmd.obj_id)
                return
            cmd = self._add_obj_seq_if_necessary(cmd, chain)

            self._logger.debug("CMD %s", cmd)

            # fixme: need to also reject commands with sequences less
            # than the max in the pending set
            if (cmd.obj_id, cmd.obj_seq) in self._pending:
                fail('request %d:%d already exists' % (cmd.obj_id, cmd.obj_seq))
                return

            self._pending[(cmd.obj_id, cmd.obj_seq)] = cmd

            state = CallState(
                self._my_id,
                partial(self._handle_ok, resp, cmd),
                partial(self._handle_fail, fail, cmd),
                call_peer,
                cmd,
                chain,
                self._store,
                self._logger,
            )
            state._cmd_prepare()

    def _add_obj_seq_if_necessary(self, cmd, chain):
        """Returns a Command with an obj_seq if this is the head."""

        is_head = chain[0] == self._my_id
        if is_head:
            max_seq = self._seq_map.get(cmd.obj_id) or self._max_seq(cmd.obj_id)
            obj_seq = max_seq + 1
            self._seq_map[cmd.obj_id] = obj_seq
            cmd = cmd._replace(obj_seq=obj_seq)

        return cmd

    def _handle_ok(self, resp, cmd, ret):
        del self._pending[(cmd.obj_id, cmd.obj_seq)]
        resp(ret)

    def _handle_fail(self, fail, cmd, error):
        debug_msg = '%s -- %s' % (cmd.debug_tag, error)
        self._logger.error(debug_msg)
        fail(error)

    def set_conf(self, conf):
        self._conf = conf
        self._chain_map = self._conf['chain_map']


class ReloadConfOp(object):
    def __init__(self, handler, call_interface, logger):
        self._handler = handler
        self._call_interface = call_interface
        self._logger = logger

    def __call__(self, cmd, resp, fail):
        conf = cmd.args.get('conf')
        if not conf:
            self._logger.error('error loading config')
            fail('unable to set empty config')
            return

        self._logger.warn('loading config version %s' % conf['version'])
        self._call_interface.load_conf(conf['nodes'])
        self._handler.set_conf(conf)
        resp('OK')


def update_op(func):
    """Update (mutating) op."""

    func._op_type = 'update'
    func._op_name = func.__name__

    return func

def query_op(func):
    """Query (non-mutating) op."""

    func._op_type = 'query'
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
