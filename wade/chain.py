"""Chain replication and command interface logic.

"""

import logging
import traceback
from collections import namedtuple
from functools import partial

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
                     ])



class RespondError(Exception):
    def __init__(self, message):
        self.message = message


class SpecialOpError(RespondError):
    def __init__(self, message):
        self.message = message


class CallState(object):
    """Represents an attempt to process and update or query command.

    CallState transitions a command through a few states:

    1. Sanitization: Checks to make sure all parameters make sense and
    calculates some useful derived properties.

    2. Prepare: Adds command to pending set and forwards to next node
    in chain if necessary. If not then skips to commit. A response or
    timeout from the successor transitions us appropriately to commit
    or finalize.

    3. Commit: Applies the op if it's an update, or generates a
    response if it's a query. Manipulates pending set then returns
    response.

    """

    def __init__(
            self,
            my_id,
            call_peer,
            respond,
            cmd,
            chain_map,
            pending,
            store,
            logger,
            accept_updates,
    ):
        self._my_id = my_id
        self._call_peer = call_peer
        self._respond = respond
        self._cmd = cmd
        self._chain_map = chain_map
        self._pending = pending
        self._store = store
        self._logger = logger
        self._accept_updates = accept_updates

    def state_sanitize(self):
        """Check command makes sense and pre-calculate some useful
        properties.

        """

        try:
            self._func = self._store.get_op(self._cmd.op)
            if not self._func:
                raise RespondError('no function defined for op: %s' % self._cmd.op)

            self._chain = self._chain_map.get(self._cmd.obj_id)
            if not self._chain:
                raise RespondError('no chain for obj %s' % self._cmd.obj_id)

            self._in_chain = self._my_id in self._chain
            self._is_head = self._chain[0] == self._my_id
            self._is_tail = self._chain[-1] == self._my_id
            self._is_body = self._in_chain and not (self._is_head or self._is_tail)
            self._obj_seq_assigned = self._cmd.obj_seq != -1
            self._next_node_id = None

            if self._func._op_type == UPDATE_OP:
                self._check_update()
                self._assign_obj_seq()
                self._add_to_pending()

            if self._func._op_type == QUERY_OP:
                self._check_query()
        except RespondError as resp:
            self._remove_from_pending()
            self._respond(chorus.ERR, resp.message)
            return

        self.state_prepare()

    def state_prepare(self):
        """Prepares and forwards command if necessary, otherwise skips
        directly to commit.

        """

        if self._func._op_type == UPDATE_OP and not self._is_tail:
            # update not at tail, so forward
            my_index = self._chain.index(self._my_id)
            self._next_node_id = self._chain[my_index + 1]

            self._call_peer(
                self._next_node_id,
                self._cmd._asdict(),
                self.state_commit,
                timeout=1.0,
            )
        else:
            self._next_node_id = None
            self.state_commit(chorus.OK, None)

    def state_commit(self, status, message):
        """Applies op, manipulates pending set if necessary, and returns
        response.

        """

        try:
            if status != chorus.OK:
                raise RespondError(
                    '%d forward to %d failed: %s' % \
                    (self._my_id, self._next_node_id, message)
                )

            if self._func._op_type == UPDATE_OP:
                cur_seq = self._store.max_seq(self._cmd.obj_id)
                if self._cmd.obj_seq != cur_seq + 1:
                    self._remove_from_pending()
                    raise RespondError(
                        '%d out of order commit attempt, cur = %d, attempt = %d' % \
                        (self._my_id, cur_seq, self._cmd.obj_seq)
                    )

            ret = self._func(
                self._cmd.obj_id,
                self._cmd.obj_seq,
                self._cmd.args,
            )
        except RespondError as resp:
            self._respond(chorus.ERR, resp.message)
            return
        except Exception:
            self._respond(chorus.ERR, traceback.format_exc())
            return

        self._remove_from_pending()
        self._respond(chorus.OK, ret)

    def _check_update(self):
        """Check that update command makes sense."""

        if not self._accept_updates:
            raise RespondError('node currently not accepting updates')

        # If we're the head and this is an update command, the obj_seq
        # should not have already been assigned. If we're not at the
        # head then we expect the obj_seq to be assigned.
        if self._is_head and self._obj_seq_assigned:
            raise RespondError(
                'update at head already assigned seq, obj_id %s, chain: %s' % \
                (self._cmd.obj_id, self._chain)
            )

        if not self._is_head and not self._obj_seq_assigned:
            raise RespondError(
                'update command not at head, obj_id %s, chain: %s' % \
                (self._cmd.obj_id, self._chain)
            )

        # So to restate the logic from the above two if statements.
        # Once we get past them, we're in one of two possible valid
        # states:
        #
        # 1. We are at the head and there is no obj_seq.
        # 2. We are not at the head and there *is* an obj_seq.

    def _assign_obj_seq(self):
        """Assign obj_seq if we're at the head."""

        if not self._is_head:
            return

        try:
            max_seq = max(seq for obj_id, seq in self._pending.iterkeys())
        except ValueError:
            max_seq = self._store.max_seq(self._cmd.obj_id)
        obj_seq = max_seq + 1
        self._cmd = self._cmd._replace(obj_seq=obj_seq)

    def _add_to_pending(self):
        """Add command to pending set."""

        # fixme: need to also reject commands with sequences less
        # than the max in the pending set
        if (self._cmd.obj_id, self._cmd.obj_seq) in self._pending:
            raise RespondError(
                'request %d:%d already exists' % (self._cmd.obj_id, self._cmd.obj_seq)
            )

        self._pending[(self._cmd.obj_id, self._cmd.obj_seq)] = self._cmd

    def _remove_from_pending(self):
        """Remove command from pending set."""

        try:
            del self._pending[(self._cmd.obj_id, self._cmd.obj_seq)]
        except KeyError:
            pass

    def _check_query(self):
        """Check that query command makes sense."""

        if not self._is_tail:
            raise RespondError(
                'query command not at tail, obj_id %s, chain: %s' % \
                (self._cmd.obj_id, self._chain)
            )


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

        self._pending = {}
        self._chain_map = {}
        self._conf = {}
        self._accept_updates = True

        # special ops tell the node to do something, such as stop
        # reload config, stop accepting requests, but aren't regular
        # object commands
        self._special_ops = {
            '.RELOAD_CONF': partial(reload_conf_op, self, call_interface, self._logger),
            '.SET_ACCEPT_UPDATES': partial(set_accept_updates_op, self, self._logger),
            '.HEARTBEAT': lambda cmd, resp: resp(chorus.OK, 'OK'),
            '.GET_CONFIG': lambda cmd, resp: resp(chorus.OK, self._conf),
        }

    def set_accept_updates(self, accept_updates):
        self._accept_updates = accept_updates

    def __call__(self, resp, call_peer, raw_cmd):
        try:
            cmd = Command(**raw_cmd)
        except TypeError:
            resp(chorus.ERR, 'invalid command: %s' % raw_cmd)
            return

        self._logger.debug("CMD %s", cmd)

        special_func = self._special_ops.get(cmd.op)
        if special_func:
            try:
                special_func(cmd, resp)
            except SpecialOpError as e:
                self._logger.error('special op error: %s', e.message)
                resp(chorus.ERR, e.message)
            return

        state = CallState(
            self._my_id,
            call_peer,
            resp,
            cmd,
            self._chain_map,
            self._pending,
            self._store,
            self._logger,
            self._accept_updates,
        )
        state.state_sanitize()

    def set_conf(self, conf):
        self._conf = conf
        self._chain_map = self._conf['chain_map']


def reload_conf_op(handler, call_interface, logger, cmd, resp):
    conf = cmd.args.get('conf')
    if not conf:
        raise SpecialOpError('error loading config, unable to set empty config')

    logger.warning('loading config version %s' % conf['version'])
    call_interface.load_conf(conf['nodes'])
    handler.set_conf(conf)
    resp(chorus.OK, 'OK')


def set_accept_updates_op(handler, logger, cmd, resp):
    accept_updates = cmd.args.get('accept_updates')
    if accept_updates is None or not isinstance(accept_updates, bool):
        raise SpecialOpError('accept_updates is a required boolean param')

    if accept_updates:
        logger.warning('setting node to accept updates')
    else:
        logger.warning('setting node to not accept updates')
    handler.set_accept_updates(accept_updates)

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

    def update(self, op_name, obj_id, args, debug_tags):
        return self._call('UPDATE', op_name, obj_id, args, debug_tags)

    def query(self, op_name, obj_id, args, debug_tags):
        return self._call('QUERY', op_name, obj_id, args, debug_tags)

    def _call(self, op_type, op_name, obj_id, args, debug_tag):
        """Sends command to cluster."""

        if op_type == 'UPDATE':
            peer_index = 0
        if op_type == 'QUERY':
            peer_index = -1

        peer_id = self._conf['chain_map'][obj_id][peer_index]
        return self._chorus_client.reqrep(
            peer_id,
            {
                'obj_id': obj_id,
                'obj_seq': -1,
                'op': op_name,
                'args': args,
                'debug_tag': debug_tag,
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
                     })
                 for peer_id in peer_ids }
