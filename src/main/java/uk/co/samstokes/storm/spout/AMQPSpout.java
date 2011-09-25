package uk.co.samstokes.storm.spout;

import java.io.IOException;
import java.util.Map;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


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

    private transient Connection amqpConnection;
    private transient Channel amqpChannel;
    private transient QueueingConsumer amqpConsumer;
    private transient String amqpConsumerTag;

    private SpoutOutputCollector collector;


    public AMQPSpout(String host, int port, String username, String password, String vhost, String exchange, String routingKey) {
        this.amqpHost = host;
        this.amqpPort = port;
        this.amqpUsername = username;
        this.amqpPassword = password;
        this.amqpVhost = vhost;
        this.amqpExchange = exchange;
        this.amqpRoutingKey = routingKey;
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
                    // TODO do something more intelligent
                    final String todoHack = new String(message);
                    collector.emit(new Values(todoHack));
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
        // TODO do something more intelligent
        declarer.declare(new Fields("todoHack"));
    }


    @Override
    public boolean isDistributed() {
        // TODO Auto-generated method stub
        return false;
    }
}
