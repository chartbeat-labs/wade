TODO
----

- Make req_id function of connection, so clients don't have to
  generate unique req_ids.

- Stores should be able to set up periodic functions, to perform
  maintenance such as TTLs and object gc (removing object data on
  nodes that are no longer involved in its chain).

- Dogtag stats.

- Implement fast/full sync and read/write pauses.

- We create a Timer for every single call that has a timeout. The
  Timer object lives on even if the call completes. I'm guessing
  that's horrendously bad and needs a good fixing.

- Implement coordinator framework. See "chain configuration" below.

- Chain configuration. Currently, WADE expects the coordinator to send
  chain reconfigurations. How do those propagate to clients? Clients
  either grab these from some well known location (like the
  coordinator itself or ZooKeeper), or they get the configuration from
  the nodes. I'm leaning towards the latter to avoid the complexity of
  having clients understand an out-of-band configuration change. One
  way this might work is that configurations are versioned
  (sequentially, or perhaps with a hashing function to avoid
  complication), and the node verifies that its config matches the
  clients. In the case of a mismatch, the request fails and clients
  should aks for the new config from the node. Yet another alternative
  is to make clients thin and able to connect to any node; requests
  are forwarded by the node to the appropriate chain head. Clients
  still have to deal with load balancing and getting configuration
  changes, so I feel like it's worth it to tradeoff the config
  versioning complexity in order to reduce the number of network hops
  by one.

- Make synchronous client (reqrep) threadsafe. This is probably the
  single most important thing affecting performance.

- Are there msgpack message limits? Full-sync and serialization seems
  potentially rough.

- Checksum traces. Ask nodes to calculate and send back checksums of
  their object state for integrity checking.

- Return more than just call data to reqrep client. Also return timing
  stats and other useful info.

- Nodes are configured to timeout their forwards after 1 second. How
  should we make this configurable?

- Error handling overall needs to be carefully thought out.

- pypy support. pyuv doesn't support pypy out of the box at the
  moment, but it's coming soon.

- Right now WADE only does command replication, but we may also want
  to make value replication an option. You want command replication
  when network bandwidth is more scarce than CPU power, which I
  believe is usually the case. You'd want value replication when CPU
  power is more scarce than network.
