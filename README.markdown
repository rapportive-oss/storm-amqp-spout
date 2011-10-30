# storm-amqp-spout: AMQP input source for Storm #

storm-amqp-spout allows a [Storm](https://github.com/nathanmarz/storm) topology
to consume an AMQP exchange as an input source.  It currently provides one
class:

 * <tt>[AMQPSpout][]</tt>: an implementation of
   [`backtype.storm.topology.IRichSpout`][IRichSpout] that connects to an AMQP
   broker, consumes the messages published to a specified AMQP exchange and
   emits them as Storm tuples.

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
      <version>0.0.3-SNAPSHOT</version>
    </dependency>
    <!-- ... -->
  </dependencies>
  <!-- ... -->
</project>
```

## Caveats ##

This is very early software.  It may break and the API is liable to change
completely between releases.  Pull requests, patches and bug reports are very
welcome.

This should not currently be used where guaranteed message processing is
required, because it binds to the exchange using a temporary queue when the
topology calls <tt>[open()][]</tt> on the spout.  This means it will only
receive messages published to the exchange after the call to
<tt>[open()][]</tt>, and if the spout worker restarts or the topology is
killed, it will not receive any messages published while the worker or topology
is down.

For the same reason, this spout cannot currently be distributed among
multiple workers (each worker gets its own exclusive queue, so multiple
workers would each receive their own copy of every message).

Improvements are planned to overcome both these limitations and support
guaranteed message processing, distributed across any number of workers.
These improvements may require API changes (e.g. to specify the name of an
existing queue to consume, rather than an exchange to bind to).

## Compatibility ##

AMQPSpout has been tested with RabbitMQ 2.3.1 and 2.6.1.  It should probably work with other
versions and other AMQP brokers.


[Storm]: <https://github.com/nathanmarz/storm>
    "Storm project homepage"
[IRichSpout]: <http://nathanmarz.github.com/storm/doc/backtype/storm/topology/IRichSpout.html>
    "Javadoc for backtype.storm.topology.IRichSpout"
[open()]: <http://nathanmarz.github.com/storm/doc/backtype/storm/spout/ISpout.html#open(java.util.Map,%20backtype.storm.task.TopologyContext,%20backtype.storm.spout.SpoutOutputCollector\)>
    "Javadoc for backtype.storm.spout.ISpout.open()"
[Scheme]: <http://nathanmarz.github.com/storm/doc/backtype/storm/spout/Scheme.html>
    "Javadoc for backtype.storm.spout.Scheme"
[AMQPSpout]: <http://code.rapportive.com/storm-amqp-spout/doc/com/rapportive/storm/spout/AMQPSpout.html>
    "Javadoc for AMQPSpout"
[storm-json]: <https://github.com/rapportive-oss/storm-json>
    "JSON {,de}serialisation support for Storm"
