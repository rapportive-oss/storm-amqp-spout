package com.rapportive.storm.spout;

import java.io.IOException;
import java.util.Map;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;


/**
 * Spout to feed messages into Storm from an AMQP exchange.
 *
 * This should not currently be used where guaranteed message processing is
 * required, because of two limitations:
 *
 * <ol>
 * <li>
 * Uses a temporary queue to bind to the specified exchange when the topology
 * calls <tt>open()</tt> on the spout, so it will only receive messages
 * published to the exchange after the call to <tt>open()</tt>, and if the
 * spout worker restarts or the topology is killed, it will not receive any
 * messages published while the worker or topology is down.
 * </li>
 *
 * <li>
 * Currently auto-acks all consumed messages with the AMQP broker, and does not
 * implement Storm's reliability API, so if processing a message fails it will
 * simply be discarded.
 * </li>
 * </ol>
 *
 * Limitation 1 also means this spout cannot currently be distributed among
 * multiple workers (each worker gets its own exclusive queue, so multiple
 * workers would each receive their own copy of every message).
 *
 * Improvements are planned to overcome both these limitations and support
 * guaranteed message processing, distributed across any number of workers.
 * These improvements may require API changes (e.g. to specify the name of an
 * existing queue to consume, rather than an exchange to bind to).
 *
 * @author Sam Stokes (sam@rapportive.com)
 */
public class AMQPSpout implements IRichSpout {
    private static final long serialVersionUID = 11258942292629263L;

    private static final long WAIT_FOR_NEXT_MESSAGE = 50L;

    private final String amqpHost;
    private final int amqpPort;
    private final String amqpUsername;
    private final String amqpPassword;
    private final String amqpVhost;
    private final String amqpExchange;
    private final String amqpRoutingKey;

    private final Scheme serialisationScheme;

    private transient Connection amqpConnection;
    private transient Channel amqpChannel;
    private transient QueueingConsumer amqpConsumer;
    private transient String amqpConsumerTag;

    private SpoutOutputCollector collector;


    /**
     * Create a new AMQP spout.  When
     * {@link #open(Map, TopologyContext, SpoutOutputCollector)} is called, it
     * will create a new server-named, exclusive, auto-delete queue, bind it to
     * the specified exchange on the specified server with the specified
     * routing key, and start consuming messages.  It will use the provided
     * <tt>scheme</tt> to deserialise each AMQP message into a Storm tuple.
     *
     * @param host  hostname of the AMQP broker node
     * @param port  port number of the AMQP broker node
     * @param username  username to log into to the broker
     * @param password  password to authenticate to the broker
     * @param vhost  vhost on the broker
     * @param exchange  exchange to bind to
     * @param routingKey  routing key for the binding
     * @param scheme  {@link backtype.storm.spout.Scheme} used to deserialise
     *          each AMQP message into a Storm tuple
     */
    public AMQPSpout(String host, int port, String username, String password, String vhost, String exchange, String routingKey, Scheme scheme) {
        this.amqpHost = host;
        this.amqpPort = port;
        this.amqpUsername = username;
        this.amqpPassword = password;
        this.amqpVhost = vhost;
        this.amqpExchange = exchange;
        this.amqpRoutingKey = routingKey;

        this.serialisationScheme = scheme;
    }


    @Override
    public void ack(Object arg0) {
        // TODO Auto-generated method stub

    }


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
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        try {
            if (amqpConnection != null) {
              amqpConnection.close();
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    @Override
    public void fail(Object arg0) {
        // TODO Auto-generated method stub

    }


    @Override
    public void nextTuple() {
        if (amqpConsumer != null) {
            while (true) {
                try {
                    final QueueingConsumer.Delivery delivery = amqpConsumer.nextDelivery(WAIT_FOR_NEXT_MESSAGE);
                    if (delivery == null) break;
                    final byte[] message = delivery.getBody();
                    collector.emit(serialisationScheme.deserialize(message));
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }


    @Override
    public void open(@SuppressWarnings("rawtypes") Map config, TopologyContext context, SpoutOutputCollector collector) {
        try {
            this.collector = collector;

            setupAMQP();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    private void setupAMQP() throws IOException {
        final ConnectionFactory connectionFactory = new ConnectionFactory();

        connectionFactory.setHost(amqpHost);
        connectionFactory.setPort(amqpPort);
        connectionFactory.setUsername(amqpUsername);
        connectionFactory.setPassword(amqpPassword);
        connectionFactory.setVirtualHost(amqpVhost);

        this.amqpConnection = connectionFactory.newConnection();
        this.amqpChannel = amqpConnection.createChannel();

        // TODO qos (avoid huge internal queue)

        amqpChannel.exchangeDeclarePassive(amqpExchange);

        /*
         * This declares an exclusive, auto-delete, server-named queue.  It'll
         * be deleted when this connection closes, e.g. if the spout worker
         * process gets restarted.  That means we won't receive messages sent
         * before the connection was opened or after it was closed, which may
         * not be the desired behaviour.
         *
         * To avoid this, we want a named queue that can stick around while we
         * get rebooted.  That's hard to do without risking clashing with queue
         * names on the server.  Maybe we should have an overridable method for
         * declaring the queue?
         *
         * This actually affects isDistributed() - if we have a named queue,
         * then several workers can share the same queue, and the broker will
         * round-robin between them.  If we use server-named queues, each
         * worker will get its own, so the broker will broadcast to all of them
         * instead.
         */
        final String queue = amqpChannel.queueDeclare().getQueue();

        amqpChannel.queueBind(queue, amqpExchange, amqpRoutingKey);

        this.amqpConsumer = new QueueingConsumer(amqpChannel);
        /*
         * This tells the consumer to auto-ack every message as soon as we get
         * it.  For reliability, we probably want to manually ack or reject
         * (e.g. in ack() and fail()).
         */
        this.amqpConsumerTag = amqpChannel.basicConsume(queue, true, amqpConsumer);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(serialisationScheme.getOutputFields());
    }


    /**
     * Currently this spout can't be distributed, because each call to open()
     * creates a new queue and binds it to the exchange, so if there were
     * multiple workers they would each receive every message.
     */
    @Override
    public boolean isDistributed() {
        return false;
    }
}
