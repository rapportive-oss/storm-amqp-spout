# storm-amqp-spout: AMQP input source for Storm #

storm-amqp-spout allows a [Storm](https://github.com/nathanmarz/storm) topology
to consume an AMQP exchange as an input source.  It currently provides one
class:

 * [AMQPSpout](http://code.rapportive.com/storm-amqp-spout/doc/com/rapportive/storm/spout/AMQPSpout.html):
   an implementation of
   [`backtype.storm.topology.IRichSpout`](http://nathanmarz.github.com/storm/doc/backtype/storm/topology/IRichSpout.html)
   that connects to an AMQP broker and consumes the messages published to a
   specified AMQP exchange.

You'll need to provide a
[Scheme](http://nathanmarz.github.com/storm/doc/backtype/storm/spout/Scheme.html)
to tell `AMQPSpout` how to interpret the messages and turn them into Storm
tuples.  See e.g. [storm-json](https://github.com/rapportive-oss/storm-json) if
your messages are JSON.

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
      <version>0.0.1</version>
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
required, because of two limitations:

1. Uses a temporary queue to bind to the specified exchange when the topology
calls
[`open()`](http://nathanmarz.github.com/storm/doc/backtype/storm/spout/ISpout.html#open(java.util.Map,%20backtype.storm.task.TopologyContext,%20backtype.storm.spout.SpoutOutputCollector))
on the spout, so it will only receive messages published to the exchange after
the call to `open()`, and if the spout worker restarts or the topology is
killed, it will not receive any messages published while the worker or topology
is down.

2. Currently auto-acks all consumed messages with the AMQP broker, and does not
implement Storm's reliability API, so if processing a message fails it will
simply be discarded.

Limitation 1 also means this spout cannot currently be distributed among
multiple workers (each worker gets its own exclusive queue, so multiple
workers would each receive their own copy of every message).

Improvements are planned to overcome both these limitations and support
guaranteed message processing, distributed across any number of workers.
These improvements may require API changes (e.g. to specify the name of an
existing queue to consume, rather than an exchange to bind to).

`AMQPSpout` has been tested with RabbitMQ.  It should probably work with other
AMQP brokers.
