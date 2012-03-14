package com.rapportive.storm.amqp;

import java.io.IOException;

import com.rabbitmq.client.AMQP.Queue;

import com.rabbitmq.client.Channel;

/**
 * Declares a named, durable queue and binds it to an existing exchange.  This
 * is a good choice for production use as the queue will survive spout
 * restarts, so you won't miss messages if your spout crashes.
 *
 * <p><strong>N.B.</strong> this could be risky under some circumstances. e.g.
 * if while prototyping you set a development topology consuming from a
 * production AMQP server, then kill your topology and go home for the night;
 * messages will continue to be queued up, which could threaten the stability
 * of the AMQP server if the exchange is high-volume.  For prototyping consider
 * {@link ExclusiveQueueWithBinding}.</p>
 *
 * <p>This queue is safe for multiple parallel spout tasks: as they all consume
 * the same named queue, the AMQP broker will round-robin messages between
 * them, so each message will get processed only once (barring redelivery due
 * to outages).</p>
 */
public class SharedQueueWithBinding implements QueueDeclaration {
    private static final long serialVersionUID = 2364833412534518859L;

    private final String queueName;
    private final String exchange;
    private final String routingKey;
    private HAPolicy haPolicy;

    /**
     * Create a declaration of a named, durable, non-exclusive queue bound to
     * the specified exchange.
     *
     * @param queueName  name of the queue to be declared.
     * @param exchange  exchange to bind the queue to.
     * @param routingKey  routing key for the exchange binding.  Use "#" to
     *                    receive all messages published to the exchange.
     */
    public SharedQueueWithBinding(String queueName, String exchange, String routingKey) {
        this(queueName, exchange, routingKey, null);
    }

    /**
     * Create a declaration of a named, durable, non-exclusive queue bound to
     * the specified exchange.
     *
     * @param queueName  name of the queue to be declared.
     * @param exchange  exchange to bind the queue to.
     * @param routingKey  routing key for the exchange binding.  Use "#" to
     *                    receive all messages published to the exchange.
     * @param policy  high-availability policy to use
     */
    public SharedQueueWithBinding(String queueName, String exchange, String routingKey, HAPolicy policy) {
        this.queueName = queueName;
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.haPolicy = policy;
    }

    /**
     * Verifies the exchange exists, creates the named queue if it does not
     * exist, and binds it to the exchange.
     *
     * @return the server's response to the successful queue declaration.
     *
     * @throws IOException  if the exchange does not exist, the queue could not
     *                      be declared, or if the AMQP connection drops.
     */
    @Override
    public Queue.DeclareOk declare(Channel channel) throws IOException {
        channel.exchangeDeclarePassive(exchange);

        final Queue.DeclareOk queue = channel.queueDeclare(
                queueName,
                /* durable */ true,
                /* non-exclusive */ false,
                /* non-auto-delete */ false,
                haPolicy == null ? null /* no arguments */ : haPolicy.asQueueProperies());

        channel.queueBind(queue.getQueue(), exchange, routingKey);

        return queue;
    }

    /**
     * Returns <tt>true</tt> as this queue is safe for parallel consumers.
     */
    @Override
    public boolean isParallelConsumable() {
        return true;
    }
}
