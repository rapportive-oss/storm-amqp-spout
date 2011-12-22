package com.rapportive.storm.spout;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;

import com.rabbitmq.client.AMQP.Queue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

import com.rapportive.storm.amqp.QueueDeclaration;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;

import backtype.storm.utils.Utils;

/**
 * Spout to feed messages into Storm from an AMQP queue.  Each message routed
 * to the queue will be emitted as a Storm tuple.  The message will be acked or
 * rejected once the topology has respectively fully processed or failed the
 * corresponding tuple.
 *
 * <p><strong>N.B.</strong> if you need to guarantee all messages are reliably
 * processed, you should have AMQPSpout consume from a queue that is
 * <em>not</em> set as 'exclusive' or 'auto-delete': otherwise if the spout
 * task crashes or is restarted, the queue will be deleted and any messages in
 * it lost, as will any messages published while the task remains down.  See
 * {@link com.rapportive.storm.amqp.SharedQueueWithBinding} to declare a shared
 * queue that allows for guaranteed processing.  (For prototyping, an
 * {@link com.rapportive.storm.amqp.ExclusiveQueueWithBinding} may be
 * simpler to manage.)</p>
 *
 * <p><strong>N.B.</strong> this does not currently handle malformed messages
 * (which cannot be deserialised by the provided {@link Scheme}) very well:
 * the spout worker will crash if it fails to serialise a message.</p>
 *
 * <p>This consumes messages from AMQP asynchronously, so it may receive
 * messages before Storm requests them as tuples; therefore it buffers messages
 * in an internal queue.  To avoid this buffer growing large and consuming too
 * much RAM, set {@link #CONFIG_PREFETCH_COUNT}.</p>
 *
 * <p>This spout can be distributed among multiple workers, depending on the
 * queue declaration: see {@link QueueDeclaration#isParallelConsumable}.</p>
 *
 * @see QueueDeclaration
 * @see com.rapportive.storm.amqp.SharedQueueWithBinding
 * @see com.rapportive.storm.amqp.ExclusiveQueueWithBinding
 *
 * @author Sam Stokes (sam@rapportive.com)
 */
public class AMQPSpout implements IRichSpout {
    private static final long serialVersionUID = 11258942292629263L;

    private static final Logger log = Logger.getLogger(AMQPSpout.class);

    /**
     * Storm config key to set the AMQP basic.qos prefetch-count parameter.
     * Defaults to 100.
     *
     * <p>This caps the number of messages outstanding (i.e. unacked) at a time
     * that will be sent to each spout worker.  Increasing this will improve
     * throughput if the network roundtrip time to the AMQP broker is
     * significant compared to the time for the topology to process each
     * message; this will also increase the RAM requirements as the internal
     * message buffer grows.</p>
     *
     * <p>AMQP allows a prefetch-count of zero, indicating unlimited delivery,
     * but that is not allowed here to avoid unbounded buffer growth.</p>
     */
    public static final String CONFIG_PREFETCH_COUNT = "amqp.prefetch.count";
    private static final long DEFAULT_PREFETCH_COUNT = 100;

    /**
     * Time in milliseconds to wait for a message from the queue if there is
     * no message ready when the topology requests a tuple (via
     * {@link #nextTuple()}).
     */
    public static final long WAIT_FOR_NEXT_MESSAGE = 1L;

    /**
     * Time in milliseconds to wait after losing connection to the AMQP broker
     * before attempting to reconnect.
     */
    public static final long WAIT_AFTER_SHUTDOWN_SIGNAL = 10000L;

    private final String amqpHost;
    private final int amqpPort;
    private final String amqpUsername;
    private final String amqpPassword;
    private final String amqpVhost;

    private final QueueDeclaration queueDeclaration;

    private final Scheme serialisationScheme;

    private transient Connection amqpConnection;
    private transient Channel amqpChannel;
    private transient QueueingConsumer amqpConsumer;
    private transient String amqpConsumerTag;

    private SpoutOutputCollector collector;

    private int prefetchCount;


    /**
     * Create a new AMQP spout.  When
     * {@link #open(Map, TopologyContext, SpoutOutputCollector)} is called, it
     * will declare a queue according to the specified
     * <tt>queueDeclaration</tt>, subscribe to the queue, and start consuming
     * messages.  It will use the provided <tt>scheme</tt> to deserialise each
     * AMQP message into a Storm tuple.
     *
     * @param host  hostname of the AMQP broker node
     * @param port  port number of the AMQP broker node
     * @param username  username to log into to the broker
     * @param password  password to authenticate to the broker
     * @param vhost  vhost on the broker
     * @param queueDeclaration  declaration of the queue / exchange bindings
     * @param scheme  {@link backtype.storm.spout.Scheme} used to deserialise
     *          each AMQP message into a Storm tuple
     */
    public AMQPSpout(String host, int port, String username, String password, String vhost, QueueDeclaration queueDeclaration, Scheme scheme) {
        this.amqpHost = host;
        this.amqpPort = port;
        this.amqpUsername = username;
        this.amqpPassword = password;
        this.amqpVhost = vhost;
        this.queueDeclaration = queueDeclaration;

        this.serialisationScheme = scheme;
    }


    /**
     * Acks the message with the AMQP broker.
     */
    @Override
    public void ack(Object msgId) {
        if (msgId instanceof Long) {
            final long deliveryTag = (Long) msgId;
            if (amqpChannel != null) {
                try {
                    amqpChannel.basicAck(deliveryTag, false /* not multiple */);
                } catch (IOException e) {
                    log.warn("Failed to ack delivery-tag " + deliveryTag, e);
                }
            }
        } else {
            log.warn(String.format("don't know how to ack(%s: %s)", msgId.getClass().getName(), msgId));
        }
    }


    /**
     * Cancels the queue subscription, and disconnects from the AMQP broker.
     */
    @Override
    public void close() {
        try {
            if (amqpChannel != null) {
              if (amqpConsumerTag != null) {
                  amqpChannel.basicCancel(amqpConsumerTag);
              }

              amqpChannel.close();
            }
        } catch (IOException e) {
            log.warn("Error closing AMQP channel", e);
        }

        try {
            if (amqpConnection != null) {
              amqpConnection.close();
            }
        } catch (IOException e) {
            log.warn("Error closing AMQP connection", e);
        }
    }


    /**
     * Tells the AMQP broker to drop (Basic.Reject) the message.
     *
     * <p><strong>N.B.</strong> this does <em>not</em> requeue the message:
     * failed messages will simply be dropped.  This is to prevent infinite
     * redelivery in the event of non-transient failures (e.g. malformed
     * messages).  However it means that messages will <em>not</em> be retried
     * in the event of transient failures.</p>
     *
     * <p><strong>TODO</strong> make this configurable.</p>
     */
    @Override
    public void fail(Object msgId) {
        if (msgId instanceof Long) {
            final long deliveryTag = (Long) msgId;
            if (amqpChannel != null) {
                try {
                    amqpChannel.basicReject(deliveryTag, false /* don't requeue */);
                } catch (IOException e) {
                    log.warn("Failed to reject delivery-tag " + deliveryTag, e);
                }
            }
        } else {
            log.warn(String.format("don't know how to reject(%s: %s)", msgId.getClass().getName(), msgId));
        }
    }


    /**
     * Emits the next message from the queue as a tuple.
     *
     * <p>If no message is ready to emit, this will wait a short time
     * ({@link #WAIT_FOR_NEXT_MESSAGE}) for one to arrive on the queue,
     * to avoid a tight loop in the spout worker.</p>
     */
    @Override
    public void nextTuple() {
        if (amqpConsumer != null) {
            try {
                final QueueingConsumer.Delivery delivery = amqpConsumer.nextDelivery(WAIT_FOR_NEXT_MESSAGE);
                if (delivery == null) return;
                final long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                final byte[] message = delivery.getBody();
                collector.emit(serialisationScheme.deserialize(message), deliveryTag);
                /*
                 * TODO what to do about malformed messages? Skip?
                 * Avoid infinite retry!
                 * Maybe we should output them on a separate stream.
                 */
            } catch (ShutdownSignalException e) {
                log.warn("AMQP connection dropped, will attempt to reconnect...");
                Utils.sleep(WAIT_AFTER_SHUTDOWN_SIGNAL);
                reconnect();
            } catch (InterruptedException e) {
                // interrupted while waiting for message, big deal
            }
        }
    }


    /**
     * Connects to the AMQP broker, declares the queue and subscribes to
     * incoming messages.
     */
    @Override
    public void open(@SuppressWarnings("rawtypes") Map config, TopologyContext context, SpoutOutputCollector collector) {
        Long prefetchCount = (Long) config.get(CONFIG_PREFETCH_COUNT);
        if (prefetchCount == null) {
            log.info("Using default prefetch-count");
            prefetchCount = DEFAULT_PREFETCH_COUNT;
        } else if (prefetchCount < 1) {
            throw new IllegalArgumentException(CONFIG_PREFETCH_COUNT + " must be at least 1");
        }
        this.prefetchCount = prefetchCount.intValue();

        try {
            this.collector = collector;

            setupAMQP();
        } catch (IOException e) {
            log.error("AMQP setup failed", e);
        }
    }


    private void setupAMQP() throws IOException {
        final int prefetchCount = this.prefetchCount;

        final ConnectionFactory connectionFactory = new ConnectionFactory();

        connectionFactory.setHost(amqpHost);
        connectionFactory.setPort(amqpPort);
        connectionFactory.setUsername(amqpUsername);
        connectionFactory.setPassword(amqpPassword);
        connectionFactory.setVirtualHost(amqpVhost);

        this.amqpConnection = connectionFactory.newConnection();
        this.amqpChannel = amqpConnection.createChannel();

        log.info("Setting basic.qos prefetch-count to " + prefetchCount);
        amqpChannel.basicQos(prefetchCount);

        final Queue.DeclareOk queue = queueDeclaration.declare(amqpChannel);
        final String queueName = queue.getQueue();
        log.info("Consuming queue " + queueName);

        this.amqpConsumer = new QueueingConsumer(amqpChannel);
        this.amqpConsumerTag = amqpChannel.basicConsume(queueName, false /* no auto-ack */, amqpConsumer);
    }


    private void reconnect() {
        log.info("Reconnecting to AMQP broker...");
        try {
            setupAMQP();
        } catch (IOException e) {
            log.warn("Failed to reconnect to AMQP broker", e);
        }
    }


    /**
     * Declares the output fields of this spout according to the provided
     * {@link backtype.storm.spout.Scheme}.
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(serialisationScheme.getOutputFields());
    }


    /**
     * This spout can be distributed among multiple workers if the
     * {@link QueueDeclaration} supports it.
     *
     * @see QueueDeclaration#isParallelConsumable()
     */
    @Override
    public boolean isDistributed() {
        return queueDeclaration.isParallelConsumable();
    }
}
