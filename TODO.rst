TODO
----

- Stores should be able to set up periodic functions, to perform
  maintenance such as TTLs and object gc (removing object data on
  nodes that are no longer involved in its chain).

- Dogtag stats.

- Implement fast/full sync and read/write pauses.

- We create a Timer for every single call that has a timeout. The
  Timer object lives on even if the call completes. I'm guessing
  that's horrendously bad and needs a good fixing.

- Implement overlord framework. See "chain configuration" below.

- Rewrite error messages, use PostgreSQL style guide:
  http://www.postgresql.org/docs/8.1/static/error-style-guide.html

- Are there msgpack message limits? Full-sync and serialization seems
  potentially rough.

- Checksum traces. Ask nodes to calculate and send back checksums of
  their object state for integrity checking.

- Return more than just call data to reqrep client. Also return timing
  stats and other useful info.

- Nodes are configured to timeout their forwards after 1 second. How
  should we make this configurable? Perhaps there should be a
  cluster-wide "node unresponsive" timeout that both the overlord and
  nodes use.

- pypy support. pyuv doesn't support pypy out of the box at the
  moment, but it's coming soon.

- Right now WADE only does command replication, but we may also want
  to make value replication an option. You want command replication
  when network bandwidth is more scarce than CPU power, which I
  believe is usually the case. You'd want value replication when CPU
  power is more scarce than network.
