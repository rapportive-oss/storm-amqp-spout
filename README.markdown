# storm-amqp-spout: AMQP input source for Storm #

storm-amqp-spout allows a [Storm][] topology to consume an AMQP queue as an
input source.  It currently provides:

 * <tt>[AMQPSpout][]</tt>: an implementation of
   [`backtype.storm.topology.IRichSpout`][IRichSpout] that connects to an AMQP
   broker, consumes the messages routed to a specified AMQP queue and emits them
   as Storm tuples.
 * <tt>[QueueDeclaration][]</tt>: an interface that encapsulates declaring an
   AMQP queue and setting up any exchange bindings it requires, used by
   AMQPSpout to set up the queue to consume.
 * <tt>[ExclusiveQueueWithBinding][]</tt>: a QueueDeclaration suitable for
   prototyping and one-off analytics use cases.
 * <tt>[SharedQueueWithBinding][]</tt>: a QueueDeclaration suitable for
   production use cases needing guaranteed processing.

You'll need to provide a [Scheme][] to tell AMQPSpout how to interpret the
messages and turn them into Storm tuples.  See e.g. [storm-json][] if your
messages are JSON.

## Documentation ##

The Javadocs can be found at [http://code.rapportive.com/storm-amqp-spout]().

## Usage ##

To produce a jar:

    $ mvn package

To install in your local Maven repository:

    $ mvn install

To use in your `pom.xml`:

```xml
<project>
  <!-- ... -->
  <dependencies>
    <!-- ... -->
    <dependency>
      <groupId>com.rapportive</groupId>
      <artifactId>storm-amqp-spout</artifactId>
      <version>0.1.1</version>
    </dependency>
    <!-- ... -->
  </dependencies>
  <!-- ... -->
</project>
```

## Caveats ##

This is early software.  It has been used at production volumes, but not yet
for business-critical use cases.  It may break and the API is liable to change
completely between releases.  Pull requests, patches and bug reports are very
welcome.

**N.B.** if you need to guarantee all messages are reliably processed, you
should have AMQPSpout consume from a queue that is *not* set as 'exclusive' or
'auto-delete': otherwise if the spout task crashes or is restarted, the queue
will be deleted and any messages in it lost, as will any messages published
while the task remains down.  See [SharedQueueWithBinding][] to declare a
shared queue that allows for guaranteed processing.  (For prototyping, an
[ExclusiveQueueWithBinding][] may be simpler to manage.)

This does not currently handle malformed messages very well: the spout worker
will crash if the provided [Scheme][] fails to deserialise a message.

This does not currently support retrying messages in the event of transient
failure to process: any message which the topology fails to process will simply
be dropped.  This is to prevent infinite redelivery in the event of
non-transient failures (e.g. malformed messages, though see previous caveat!).
This will probably be made configurable in a future release.

## Compatibility ##

AMQPSpout has been tested with RabbitMQ 2.3.1, 2.6.1 and 2.7.0.  It should probably work with other
versions and other AMQP brokers.


[Storm]: <https://github.com/nathanmarz/storm>
    "Storm project homepage"
[IRichSpout]: <http://nathanmarz.github.com/storm/doc/backtype/storm/topology/IRichSpout.html>
    "Javadoc for backtype.storm.topology.IRichSpout"
[Scheme]: <http://nathanmarz.github.com/storm/doc/backtype/storm/spout/Scheme.html>
    "Javadoc for backtype.storm.spout.Scheme"
[AMQPSpout]: <http://code.rapportive.com/storm-amqp-spout/doc/com/rapportive/storm/spout/AMQPSpout.html>
    "Javadoc for AMQPSpout"
[QueueDeclaration]: <http://code.rapportive.com/storm-amqp-spout/doc/com/rapportive/storm/amqp/QueueDeclaration.html>
    "Javadoc for QueueDeclaration"
[ExclusiveQueueWithBinding]: <http://code.rapportive.com/storm-amqp-spout/doc/com/rapportive/storm/amqp/ExclusiveQueueWithBinding.html>
    "Javadoc for ExclusiveQueueWithBinding"
[SharedQueueWithBinding]: <http://code.rapportive.com/storm-amqp-spout/doc/com/rapportive/storm/amqp/SharedQueueWithBinding.html>
    "Javadoc for SharedQueueWithBinding"
[storm-json]: <https://github.com/rapportive-oss/storm-json>
    "JSON {,de}serialisation support for Storm"
