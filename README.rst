We're All Database Engineers
----------------------------

WADE is a programmable distributed database. It has at its core a CP
system and is constructed for reasonably low latency queries, high
throughput, algorithmic simplicity, and robust operation.

In other words, you should be able to pop open this jalopy's hood and
see what's going on as it flies over potholes down the freeway.

A guiding principle is that an engineer familiar with its concepts
should be able to read and understand the entire code base in less
than half an hour. This understandability gives it an operational
advantage over more complex databases built to handle edge cases in
smarter ways than WADE. The programmability of WADE allows it to be
customized easily to provide a specialized set of database
functionality.

WADE is closer to a framework, like Django, than a runnable database.
The programmer writes custom logic that plugs into the WADE framework,
much like how a web developer writes business logic that plugs into
the Django framework.


Why Would We Build Such a Thing?
--------------------------------

You can think of a database as an object that transitions from state
to state, where the transitions are insert and update commands. SQL
provides a fairly limited set of mutating commands, typically just
setting the columns of a row to given values. Redis supports a richer
set of mutating commands that match common operations on data
structures.

WADE is a system for replicating an object and its state transitions
across a set of nodes, and is thus a generalization of a database. The
key to WADE is that the transitions are completely customizable
functions. In places where you might have had to use a
read-write-update cycle with a regular database, you can program WADE
to perform the update in a single roundtrip.

One example is a priority queue structure in a key-value data
store. Suppose that an application maps a key to a priority queue of
items, and expects to be able to add and remove items. If your
standard database is not expressive enough to implement a priority
queue, then you might go about doing this by serializing the queue and
storing it directly in the database as an opaque value. Adding an item
to the queue would require pulling down the value to the client,
deserializing, performing the update, reserializing, and then writing
it back to the database. With WADE, the programmer would directly
implement the ``add_item`` operation in the database so that a client
could add items with a single command in a single roundtrip.

Along the same veins, a client might have to pull down the entire
serialized value and deserialize it to get the size of the
queue. Whereas with WADE, the ``size`` command would execute directly
on the node and return only the result, thereby reducing the amount of
network transfer required to answer a simple query.

The ability to run computations directly on the node without pulling
down an object's data could be a huge advantage if the size of data is
large compared to the output of the computation. For instance, you
could calculate the variance of large number of metrics without high
network overhead.


Design Principles
-----------------

::

  The smaller the object, the more forgiving we can be when it
  misbehaves.

  - John Maeda


- Simplicity above all else. We have a quota of 1,000 lines of code,
  so ease of implementation is a major factor when picking an
  algorithm or design.

- Single thread execution. WADE's performance comes from running
  multiple instances on many cores. Single threaded operation not only
  helps keep the core code speedy and simple, but also allows
  customizations to be simple as well.

- Understandable operation. There should be a minimal number tweakable
  parameters.

- Failure scenarios should be obvious, debuggable, and recoverable.

- Debuggability and profiling are first class concerns, as they effect
  ease of development and understandable operation.


State Objects
-------------

A WADE cluster consists of a set of single threaded nodes, each of
which holds one or more objects. WADE targets between a few hundred
and around a million objects per cluster. Objects are the unit of
replication, and WADE pushes each one to an ordered subset of nodes,
called a chain. The programmer defines a set of update commands that
transition objects from state to state, as well as query commands that
return a non-mutating view of an object.

As an example, consider the implementation of a simple in-memory
key-value store, WADE-KV. The command interface for WADE-KV might
consist of three operations, SET, GET, and SIZE. Suppose that the
programmer has decided that this cluster has 100 objects. WADE maps
each of these objects to a chain, and manages replicating the object
through the chain.

A WADE-KV client issues SET and GET commands to whatever object the
programmer wants. For example, a reasonable implementation would be
hashing the key, modding by 100, and then issuing a SET with arguments
(k, v) on the resulting chain. GET and SIZE request would similarly be
issued on the same chain. Since query commands are simply non-mutating
code, they don't have to return direct values from an object. They
could perform some computation, such as SIZE, which returns the number
of key-value pairs in its object.


Overlord
--------

A overlord node manages the chain and peer configuration. How the
overlord works is completely unspecified at the moment, except that
nodes implement a command to set their configuration. The expectation
is that the overlord will take configuration from ZooKeeper or
Puppet/Chef/etc, and apply those changes to the nodes. Thus, the
overlord detects node failures and adjust chains correctly with some
expediency. A chain is unavailable for writes if any of its nodes are
down.

Overlord redundancy is outside the scope of WADE. The expectation is
that the operator will use ZooKeeper, Consul, or some cluster
configurator to ensure that there always exists an overlord process.


Chain Replication
-----------------

WADE uses chain replication, though it deviates from the original paper
in a few ways:

http://www.cs.cornell.edu/home/rvr/papers/osdi04.pdf

A chain consists of an ordered sequence of nodes, where the first node
is called the head, and the last node is called the tail. Update
(mutating) commands on a chain always start at the head, and propagate
through the chain to the tail and back. On the traversal from head to
tail, the node records the *intent* to apply the operation by adding the
command into a pending set. On the traversal back from tail to head, the
node applies the operation to the object.

Two notes on terminology:

1. We say that the command is *on its way in* or *entering* as it
   traverses from head to tail, and *on its way out* or *exiting* as
   the return moves from tail back to head. The next node on the way
   in is an "inward" node, and the next node on the way out is an
   "outward" node.

2. In two-phase commit, the act of recording intent to write is called a
   *prepare*. The act of performing the write is called a *commit*.
   You'll see this terminology used in the code.

A query (non-mutating) command goes directly to the tail. The tail,
then, is the arbiter of truth in a chain -- it is the node that first
decides that an operation is committed and permanently mutates object
state. An update command that never reaches the tail is never applied.

The head assigns an increasing-by-one sequence number per object to
every command, which is useful for dealing with failure scenarios (see
Chain Breaks). Nodes only commit commands in sequential order, and the
object's backing store, if one exists, is responsible for keeping
track of the maximum observed sequence. The max sequence is a complete
record of what commands an object has processed.


Chain Breaks
------------

The original chain replication algorithm distinguishes between three
kinds of stop failures in the chain: at the head, tail, and somewhere in
between.

WADE takes a simpler unified approach. Any failure of a node in the
chain results in update commands on their way in accumulating in
pending sets until the first stopped node. On recongizing a failed
node, the overlord reorganizes the chain by removing the dead node,
and tells the head to send a fast-sync command.

First, the fast-sync causes the head to stop accepting update commands.

When the fast-sync reaches the (possibly new) tail, it commits all
valid pending commands then responds back with the max sequence. On
the way out, all nodes commit pending commands less than or equal to
the max sequence and drop any other commands. The head then notes the
new max sequence and starts accepting update commands.

WADE also provides a full-sync command, intended for emergency
repairs.  A full-sync is nearly identical to a fast-sync, the
exception being that the tail responds with a serialized form of its
object state rather than the max sequence. On the way out, nodes
overwrite their object, set the max sequence to match, and drop the
entire pending set.

Adding a node simply initiates a full-sync for simplicity, though
there are a variety of ways to be smarter than this. This is
potentially one of WADE's major drawbacks, but a source of its
simplicity. Adding a node is a special case of a node failure, so
there are fewer code paths to write and reason about. However, adding
a node introduces unavailability as chains that include it stop
accepting writes while they run the full-sync. If objects are large,
chains may be unavailable for writes for unacceptably long periods of
time.


Integrity
---------

The tail is always the arbiter of truth. Its job is to maintain full
integrity and consistency.

The tail only accepts update commands that are one greater than the
max sequence for its object. It rejects any update command that
arrives in an incorrect order.

No node accepts an update command with a sequence number that is equal
to any in its pending set, or less than or equal to the max of its
object. In other words, we reject any command that might possibly be
from the past or a misinformed node.

If the head gets into a state such that successor nodes reject all
entering commands, then we need to run a full-sync. In this situation,
it's difficult or impossible to tell whether the head is misinformed, or
some intermediate node is misinformed. We assume the tail is the arbiter
of truth, so a full-sync resets all state in the chain.

Exercises for the reader:

- Why do we have to commit all pending commands at the tail during
  fast and full syncs?

- Nodes ignore return values from successor nodes as commands exit
  except in the case of full and fast syncs. Why are they not allowed
  to do something conditioned on the return value?

- Operations run a function on commit. In addition to that, we could
  also allow executing functions on prepare. Why is this a bad idea?


Command Replication
-------------------

WADE uses command replication for two reasons:

1. Many use cases result in commands taking up fewer bytes than states
   (such as an HLL database, or a single command that might manipulate
   many keys or rows like secondary indexes). Because performance is
   upper bounded by network throughput, we take great care in keeping
   network transit to a minimum.

2. WADE is agnostic to object representation, so value replication may
   be complicated or not very well defined.


Command Interface
-----------------

Command interfaces must support:

- A set of operators.

- Serializing/deserializing object state. Actually this is a bad name.
  Object states should be convertible to a form that is msgpack
  friendly.

- max sequence for a given obj\_id.


Special Ops
-----------

Special ops are commands that are run directly on a node and never
forwarded. They're mostly administrative and debugging commands.

- Fast-sync. (not implemented)

- Full-sync. (not implemented)

- reload_config / get_config: set and get node configuration.

- accept_updates: causes head to drop update commands or allow them
  through

- pdb / inject_code: see Debugging section below.


Clients
-------

Clients can connect to any node in the cluster, and can send any node
commands. The node a client connects to acts a coordinator for
forwarding the command to the appropriate chain head.

Distributing the configuration for clients is outside the scope of
WADE.


Performance Measurements
------------------------

Baseline chorus (this is the speed of the messaging system) with naive
reqrep handles about 38k messages / sec per Amazon AWS c3 core. At the
time of this writing, a c3.large instance with 2 cores (3.5 compute
units each) costs about $75 / month and should be able to do 70k
messages / sec in parallel without issue.

Single node wade-kv with naive reqrep and no replication runs at about
25k messages / sec. Thus there's substantial overhead from WADE above
chorus.

Performance is generally bounded by the rate of socket calls. Naive
reqrep is synchronous and only reads/writes one message at a time. In
other words, a single client connecting to a WADE server causes WADE
to context switch on every call. In very simple tests, we've shown
that we can get near linear speedups by batching messages. In other
words, if the client sends 2 messages at once instead of 1,
performance doubles. WADE's request protocol and server implementation
is designed to handle this, though there currently exists no client
library that takes advantage of this capability.

Also, a note from experience developing WADE in virtual machines and
Macbooks: performance characteristics can vary quite a bit, and you
must be running your final tests on a target machine. In some cases,
WADE performs better on Linux in a virtual machine than host OS X.


Code Structure & Development
----------------------------

WADE consists of two components: the messaging layer and the chain
replication algorithm.

``wade.chorus`` is the messaging layer, and exists as a clean
abstraction apart from the chain replication. It should be possible to
replace this with other messaging protocols, such as ZeroMQ or Thrift,
without much effort. Profiling also shows that the current
implementation spends about half its time in pyuv Python code. Future
optimization efforts can concentrate on that, either improving pyuv's
Python portion, replacing it with C, or replacing the entire chorus
layer with a C/C++ program.

``wade.chain`` consists of the chain replication logic, and depends on
the chorus interface. Again, it should be possible to replace this
with another implementation if needed. A plausible reworking might be
in a more efficient language that preserves the call interface to the
command operators, such as by embedding the Python interpreter.

See the source for the nitty gritty details on how chorus and chain
work.

This repo contains a Vagrantfile which will bring up a development
environment with the necessary Python packages.

This repo also contains sample databases in the ``contrib`` directory.


Debugging
---------

WADE has a "dangerous debugging" mode, which is the
``--dangerous_debug`` flag if using ``wade.into`` helpers. This turns
on two special ops: ``PDB`` and ``INJECT_CODE``.

``PDB`` allows the programmer to stop WADE and attach pdb to a port on
the node. You can then telnet to that port and remotely run a pdb
session. Note that while this session is open, WADE completely stops
responding, even to the command that started pdb.

Unfortunately, telnet doesn't give much in the way of being able to
use all the libreadline goodness we're accustomed to everywhere
else. To slightly compensate for this, ``INJECT_CODE`` allows the
programmer to write Python code and directly inject + execute it in
the special ops handler. The function gets access to a local variable,
``handler``, which is the ``chain.Handler`` instance.
``chain.Handler``, in turn, has access to everything else that should
be useful for debugging. See the code for documentation on how to use
this awesome feature.

Of course both of these functions are dangerous security risks, and so
they're turned off by default.


Things To Understand
--------------------

Some helpful things to understand when looking at the source:

- Chain replication, natch

- pyuv

- ZooKeeper


Future
------

How might we do transactions? RAMP is one possibility.

http://www.bailis.org/papers/ramp-sigmod2014.pdf

The RAMP paper pushes replication to other methods. So one way to view
the solution would be that WADE and chain replication maintain the
consistency of a RAMP partition. Then WADE logic implements the
prepare/commit parts of the RAMP protocol as commands.

One downside with RAMP is that it requires two roundtrips for
writes. The client sends prepares to all partitions (one roundtrip),
then sends commits to on receipt of prepare acknowledgements (second
roundtrip). We can modify chain replication so that all partitions
involved in a transaction, including ones that deal with replication,
are concatenated into a single long chain. The tail then assumes
responsibility of deciding when all partitions have been prepared;
partitions commit as the command exits the chain. This effectively
merges WADE's prepare/commit phases with RAMP's.

Also, note the TODO.rst file which contains less far-ranging tasks.


Authors
-------

* `Wes Chow <https://github.com/wesc>`_
* `Adrian Kramer <https://github.com/ackramer>`_
