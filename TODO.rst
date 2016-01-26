TODO
----

- Dogtag stats.

- Implement fast/full sync and read/write pauses.

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
