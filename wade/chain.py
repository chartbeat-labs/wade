"""Chain replication and command interface logic.

"""

import json
import logging
import socket
import threading
import traceback
from collections import defaultdict
from collections import namedtuple
from functools import partial

import swailing

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


class RespondWithError(Exception):
    """Represents an error that should be logged and then converted into a
    chorus.ERR response.

    Is coupled tightly with swailing.Logger.

    code - error code, one word (INTERNAL, FORWARD), this is what's
           returned in chorus layer
    primary - sentence
    detail - dict of useful debug values
    hint - sentence of possible cause

    """

    def __init__(self, code, primary, detail, hint):
        self.code = code
        self.primary = primary
        self.detail = detail
        self.hint = hint

    def log(self, logger):
        logger.primary(self.primary)
        logger.detail(self.detail)
        logger.hint(self.hint)


class CallState(object):
    """Represents an attempt to process and update or query command.

    CallState transitions a command through a few states:

    1. Sanitization: Checks to make sure all parameters make sense and
    calculates some useful derived properties.

    2. Prepare: Adds command to pending set and forwards to next node
    in chain if necessary. If not then skips to commit. A response
    from the successor transitions us appropriately to commit or
    finalize.

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
            accept_updates_by_chain,
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
        self._accept_updates_by_chain = accept_updates_by_chain

        self._func = None

    def state_sanitize(self):
        """Check command makes sense and pre-calculate some useful
        properties.

        """

        try:
            if self._cmd.op == 'FULL_SYNC':
                self._op_type = SYNC_OP
            else:
                self._func = self._store.get_op(self._cmd.op)
                self._op_type = self._func._op_type
                if not self._func:
                    raise RespondWithError(
                        'INTERNAL',
                        "undefined op: %s" % self._cmd.op,
                        {
                            'id': self._my_id,
                            'op': self._cmd.op,
                        },
                        "Client and node versions possibly differ.",
                    )

            self._chain = self._chain_map.get(self._cmd.obj_id)
            if not self._chain:
                raise RespondWithError(
                    'INTERNAL',
                    "cannot find chain for obj_id %s" % self._cmd.obj_id,
                    {
                        'id': self._my_id,
                        'obj_id': self._cmd.obj_id,
                    },
                    "Client and node may have differing configurations.",
                )

            self._in_chain = self._my_id in self._chain
            self._is_head = self._chain[0] == self._my_id
            self._is_tail = self._chain[-1] == self._my_id
            self._is_body = self._in_chain and not (self._is_head or self._is_tail)
            self._obj_seq_assigned = self._cmd.obj_seq != -1
            self._inward_node_id = None

            if self._op_type == UPDATE_OP:
                self._check_update()
                self._assign_obj_seq()
                self._add_to_pending()
            elif self._op_type == QUERY_OP:
                self._check_query()
        except RespondWithError as resp:
            self._remove_from_pending()
            with self._logger.error() as logger:
                resp.log(logger)
            self._respond(chorus.ERR, resp.code)
            return

        self.state_prepare()

    def state_prepare(self):
        """Prepares and forwards command if necessary, otherwise skips
        directly to commit.

        """
        # If a sync command traverses a chain it disables updates for that chain
        # on the way in.
        if self._op_type == SYNC_OP:
            self._accept_updates_by_chain[self._cmd.obj_id] = False

        if self._op_type in (UPDATE_OP, SYNC_OP) and not self._is_tail:
            # update not at tail, so forward
            my_index = self._chain.index(self._my_id)
            self._inward_node_id = self._chain[my_index + 1]

            self._call_peer(
                self._inward_node_id,
                self._cmd._asdict(),
                self.state_commit,
            )
        else:
            self._inward_node_id = None
            self.state_commit(chorus.OK, None)

    def state_commit(self, status, message):
        """Applies op, manipulates pending set if necessary, and returns
        response.

        """

        try:
            if status != chorus.OK:
                hints = []
                if status == chorus.PEER_DISCONNECT:
                    hints.append("Inward node possibly disconnected or died.")
                if self._inward_node_id is not None:
                    hints.append("Check inward node's error logs.")
                if not hints:
                    hints = ["Not sure."]

                raise RespondWithError(
                    'FORWARD',
                    "forward failed",
                    {
                        'id': self._my_id,
                        'inward_node_id': self._inward_node_id,
                        'obj_id': self._cmd.obj_id,
                        'obj_seq': self._cmd.obj_seq,
                        'status': status,
                        'message': message,
                    },
                    ' '.join(hints),
                )

            if self._op_type == SYNC_OP:
                # At the tail serialize object state. At other nodes overwrite
                # object. All nodes forward state to outer nodes.
                if self._is_tail:
                    ret = self._store.serialize_obj(self._cmd.obj_id)
                else:
                    ret = message
                    self._store.deserialize_obj(self._cmd.obj_id, value=ret)

                # Forward the object state to outer nodes but at the head
                # return OK to the chain.Client.
                if self._is_head:
                    ret = "successful full sync for chain %s, "\
                          "updates reenabled on this chain" % self._cmd.obj_id

                # Drop all pending commits for this object id and reenable
                # updates.
                self._remove_from_pending(all_sequences=True)
                self._accept_updates_by_chain[self._cmd.obj_id] = True

            elif self._op_type == UPDATE_OP:
                cur_seq = self._store.max_seq(self._cmd.obj_id)
                if self._cmd.obj_seq != cur_seq + 1:
                    self._remove_from_pending()
                    raise RespondWithError(
                        'INTERNAL',
                        'out of order commit attempt',
                        {
                            'id': self._my_id,
                            'current': cur_seq,
                            'attempt': self._cmd.obj_seq,
                            'obj_id': self._cmd.obj_id,
                        },
                        "Previous update may have aborted mid-chain or " \
                        "client/server have inconsistent configurations.",
                    )

            if self._op_type in (QUERY_OP, UPDATE_OP):
                ret = self._func(
                    self._cmd.obj_id,
                    self._cmd.obj_seq,
                    self._cmd.args,
                )
        except RespondWithError as resp:
            with self._logger.error() as logger:
                resp.log(logger)

            self._respond(chorus.ERR, resp.code)
            return
        except Exception:
            resp = RespondWithError(
                'INTERNAL',
                'unhandled exception',
                {
                    'id': self._my_id,
                    'traceback': traceback.format_exc(),
                },
                "Custom store failed in some way.",
            )
            with self._logger.error() as logger:
                resp.log(logger)

            self._respond(chorus.ERR, resp.code)
            return

        self._remove_from_pending()
        self._respond(chorus.OK, ret)

    def _check_update(self):
        """Check that update command makes sense."""

        if not self._accept_updates:
            raise RespondWithError(
                'INTERNAL',
                'not accepting updates on node',
                {
                    'id': self._my_id,
                    'accept_updates': self._accept_updates,
                },
                "Previously received command to stop accepting updates.",
            )

        if not self._accept_updates_by_chain[self._cmd.obj_id]:
            raise RespondWithError(
                'INTERNAL',
                'not accepting updates for obj_id',
                {
                    'id': self._my_id,
                    'obj_id': self._cmd.obj_id,
                    'accept_updates': self._accept_updates_by_chain[self._cmd.obj_id],
                },
                "Previously received command to stop accepting updates.",
            )

        # If we're the head and this is an update command, the obj_seq
        # should not have already been assigned. If we're not at the
        # head then we expect the obj_seq to be assigned.
        if self._is_head and self._obj_seq_assigned:
            raise RespondWithError(
                'FORWARD',
                'update command already assigned seq',
                {
                    'id': self._my_id,
                    'obj_id': self._cmd.obj_id,
                    'chain': self._chain,
                    # fixme: would be nice to have the outward node id
                },
                "Nodes may have inconsistent configurations.",
            )

        if not self._is_head and not self._obj_seq_assigned:
            raise RespondWithError(
                'FORWARD',
                'update command missing seq',
                {
                    'id': self._my_id,
                    'obj_id': self._cmd.obj_id,
                    'chain': self._chain,
                    # fixme: would be nice to have the outward node id
                },
                "Nodes may have inconsistent configurations.",
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
            max_seq = max(seq for obj_id, seq in self._pending.iterkeys()
                              if obj_id == self._cmd.obj_id)
        except ValueError:
            max_seq = self._store.max_seq(self._cmd.obj_id)
        obj_seq = max_seq + 1
        self._cmd = self._cmd._replace(obj_seq=obj_seq)

    def _add_to_pending(self):
        """Add command to pending set."""

        # fixme: need to also reject commands with sequences less
        # than the max in the pending set
        if (self._cmd.obj_id, self._cmd.obj_seq) in self._pending:
            raise RespondWithError(
                'INTERNAL',
                'request aready exists',
                {
                    'id': self._my_id,
                    'obj_id': self._cmd.obj_id,
                    'obj_seq': self._cmd.obj_seq,
                    'chain': self._chain,
                },
                "Possible existence of simultaneous heads or " \
                "old head didn't failover cleanly."
            )

        self._pending[(self._cmd.obj_id, self._cmd.obj_seq)] = self

    def _remove_from_pending(self, all_sequences=False):
        """Remove command from pending set."""
        if not all_sequences:
            key = (self._cmd.obj_id, self._cmd.obj_seq)
            self._logger.debug('removing %s from pending set', key)
            try:
                del self._pending[key]
            except KeyError:
                pass
        else:
            for key in self._pending.keys():
                curr_obj_id, _ = key
                if curr_obj_id == self._cmd.obj_id:
                    self._logger.debug('removing %s from pending set', key)
                    try:
                        del self._pending[key]
                    except KeyError:
                        pass

    def _check_query(self):
        """Check that query command makes sense."""

        if not self._is_tail:
            raise RespondWithError(
                'INTERNAL',
                'not allowed to answer query command',
                {
                    'id': self._my_id,
                    'obj_id': self._cmd.obj_id,
                    'chain': self._chain,
                },
                "Client/server configuration possibly inconsistent.",
            )


class Handler(object):
    """Call hander.

    Sets up a CallState and passes on control. Also takes care of the
    pending set.

    """

    def __init__(self, my_id, store, call_interface, allow_dangerous_debugging):
        self._logger = swailing.Logger(logging.getLogger('wade.chain'),
                                       5,
                                       100,
                                       structured_detail=True)

        self._my_id = my_id
        self._store = store
        self._call_interface = call_interface

        self._pending = {}
        self._chain_map = {}
        self._conf = {}
        self._accept_updates = True
        self._accept_updates_by_chain = defaultdict(lambda: True) # obj_id -> bool
        self._allow_dangerous_debugging = allow_dangerous_debugging

        # special ops tell the node to do something, such as stop
        # reload config, stop accepting requests, but aren't regular
        # object commands
        self._special_ops = {
            '.RELOAD_CONFIG': partial(reload_config_op, self, call_interface, self._logger),
            '.GET_CONFIG': lambda cmd, resp: resp(chorus.OK, self._conf),
            '.SET_ACCEPT_UPDATES': partial(set_accept_updates_op, self, self._logger),
            '.HEARTBEAT': lambda cmd, resp: resp(chorus.OK, 'OK'),
        }

        if self._allow_dangerous_debugging:
            self._special_ops.update(
                {
                    '.PDB': partial(start_remote_pdb_op, self, self._logger),
                    '.INJECT_CODE': partial(inject_code_op, self, self._logger),
                }
            )

    def set_accept_updates(self, accept_updates, obj_id=None):
        if obj_id:
            self._accept_updates_by_chain[obj_id] = accept_updates
        else:
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
            except Exception as e:
                self._logger.exception(e)
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
            self._accept_updates_by_chain,
        )
        state.state_sanitize()

    def set_conf(self, conf):
        self._conf = conf
        self._chain_map = self._conf['chain_map']

    def get_periodics(self):
        """Returns a list of (period, callable) tuples of things to run on a
        periodic basis.

        The underlying transport mechanism is responsible for calling
        the callables every period or more seconds.

        """

        return self._store.get_periodic_ops()


def reload_config_op(handler, call_interface, logger, cmd, resp):
    conf = cmd.args.get('conf')
    if not conf:
        raise ValueError('error loading config, unable to set empty config')

    logger.warning('loading config version %s' % conf['version'])
    call_interface.load_conf(conf['nodes'])
    handler.set_conf(conf)
    resp(chorus.OK, 'OK')


def set_accept_updates_op(handler, logger, cmd, resp):
    accept_updates = cmd.args.get('accept_updates')
    obj_id = cmd.args.get('obj_id')

    if accept_updates is None or not isinstance(accept_updates, bool):
        raise ValueError('accept_updates is a required boolean param')

    if obj_id:
        if accept_updates:
            logger.warning('setting chain/obj %s to accept updates', obj_id)
        else:
            logger.warning('setting chain/obj %s to reject updates', obj_id)
        handler.set_accept_updates(accept_updates, obj_id=obj_id)
    else:
        if accept_updates:
            logger.warning('setting node to accept updates')
        else:
            logger.warning('setting node to not accept updates')
        handler.set_accept_updates(accept_updates)

    resp(chorus.OK, 'OK')


def start_remote_pdb_op(handler, logger, cmd, resp):
    """Starts remote debugger on specified port."""

    from remote_pdb import RemotePdb

    port = cmd.args['port']
    logger.warning("starting remote debugger on port %d", port)

    RemotePdb('0.0.0.0', port).set_trace()
    resp(chorus.OK, 'OK')


def inject_code_op(handler, logger, cmd, cont):
    """Takes injected code and executes in server context.

    This is obviously dangerous and should only be used for debugging
    purposes. The injected code gets a reference to the handler, from
    which it should be able to reach everything else.

    The Python code runs within this function context, so it has
    access to all of this function's variables directly in its own
    scope. It should set the 'response' variable to return something
    useful to the calling client.

    """

    code = cmd.args['code']

    logger.warning("injecting code: %s", code)

    response = "!! .INJECT_CODE: code should set 'response' variable'"
    exec(compile(code, '.INJECT_CODE', 'exec'))
    cont(chorus.OK, response)


UPDATE_OP = 1
QUERY_OP = 2
PERIODIC_OP = 3
SYNC_OP = 4

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

def periodic_op(period):
    """Periodic op.

    Period is in seconds.

    """

    def inner(func):
        func._op_type = PERIODIC_OP
        func._op_name = func.__name__
        func._op_period = period
        return func

    return inner

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
        func = getattr(self, op_name, None)
        if func and hasattr(func, '_op_type'):
            return func

        return None

    def get_periodic_ops(self):
        """Returns sequence of (period, func) ops."""

        for func in [getattr(self, name) for name in dir(self)]:
            if getattr(func, '_op_type', None) == PERIODIC_OP:
                yield (func._op_period, func)


class Client(object):
    """Chain client.

    Use to communicate with chain nodes.

    """

    def __init__(self, conf, timeout):
        self._conf = conf
        self._chorus_client = chorus.Client(self._conf['nodes'], timeout)
        self._peer_ids = self._conf['nodes'].keys()

    def close(self):
        self._chorus_client.close()

    def update(self, op_name, obj_id, args, debug_tags):
        return self._call('UPDATE', op_name, obj_id, args, debug_tags)

    def query(self, op_name, obj_id, args, debug_tags):
        return self._call('QUERY', op_name, obj_id, args, debug_tags)

    def full_sync(self, obj_id, debug_tags='sync'):
        return self._call('SYNC', 'FULL_SYNC', obj_id, {}, debug_tags)

    def _call(self, op_type, op_name, obj_id, args, debug_tag):
        """Sends command to cluster."""

        if op_type in ('UPDATE', 'SYNC'):
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

        resps = {}
        resps_lock = threading.Lock()
        req_threads = []

        # make requests asynchronously to all peers. Timeouts handled by client.
        for peer_id in peer_ids:
            def make_request(p_id):
                try:
                    r = self._chorus_client.reqrep(
                         p_id,
                         {
                             'obj_id': None,
                             'obj_seq': None,
                             'op': op_name,
                             'args': args,
                             'debug_tag': tag,
                         }
                    )
                except socket.error:
                    r = 'could not connect to peer'

                with resps_lock:
                    resps[p_id] = r

            t = threading.Thread(target=make_request, args=(peer_id,))
            req_threads.append(t)
            t.start()

        for t in req_threads:
            t.join()

        return resps
