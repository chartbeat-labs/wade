WADE/KV is a simple in-memory key value store, used as a testbed for
exploring the programming interface.

You start up individual nodes by running:

::

  wade-kv --id NODE_ID --conf test-kv.conf

Then configure them with:

::

   configure-kv --conf test-kv.conf

Which will contact each node listed in ``test-kv.conf`` and give it
the location of its peers and its chain map. The idea is that in a
production environment, starting up ``wade-kv`` and configuring it
would be handled by Puppet and/or ZooKeeper orchestration.

You can set/get keys with the ``write-kv`` command. You can run a load
test with ``hammer-kv``. Both these tools use ``test-kv.conf`` to
figure out the location of servers and what the valid objects are.
