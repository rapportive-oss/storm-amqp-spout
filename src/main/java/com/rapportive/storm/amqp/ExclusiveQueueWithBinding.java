package com.rapportive.storm.amqp;

import java.io.IOException;

import com.rabbitmq.client.AMQP.Queue;

import com.rabbitmq.client.Channel;

/**
 * Declares an exclusive, server-named queue and binds it to an existing
 * exchange.  This is probably the easiest way to start prototyping with an
 * {@link com.rapportive.storm.spout.AMQPSpout}: if your app already publishes
 * to an exchange, you can just point this at the exchange and start consuming
 * messages.
 *
 * <p>However <strong>N.B.</strong> this queue setup <em>is not reliable</em>,
 * in that if the spout task crashes or restarts, messages published while the
 * spout is down will be lost (because the spout creates the queue when it
 * starts up, and the server deletes the queue when the spout closes).</p>
 *
 * <p>It also cannot scale out to multiple parallel spout tasks.  The semantics
 * of an exclusive queue mean that each spout task would get its own queue
 * bound to the exchange.  That means each task would receive a copy of every
 * message, so messages would get processed multiple times.</p>
 *
 * <p>If you need guaranteed processing or a horizontally scalable spout,
 * consider {@link SharedQueueWithBinding}.</p>
 */
public class ExclusiveQueueWithBinding implements QueueDeclaration {
    private static final long serialVersionUID = 7923072289071634425L;

    private final String exchange;
    private final String routingKey;

    /**
     * Create a declaration of an exclusive server-named queue bound to the
     * specified exchange.
     *
     * @param exchange  exchange to bind the queue to.
     * @param routingKey  routing key for the exchange binding.  Use "#" to
     *                    receive all messages published to the exchange.
     */
    public ExclusiveQueueWithBinding(String exchange, String routingKey) {
        this.exchange = exchange;
        this.routingKey = routingKey;
    }

    /**
     * Verifies the exchange exists, creates an exclusive, server-named queue
     * and binds it to the exchange.
     *
     * @return the server's response to the successful queue declaration (you
     *         can use this to discover the name of the queue).
     *
     * @throws IOException  if the exchange does not exist, or if the AMQP
     *                      connection drops.
     */
    @Override
    public Queue.DeclareOk declare(Channel channel) throws IOException {
        channel.exchangeDeclarePassive(exchange);

        final Queue.DeclareOk queue = channel.queueDeclare();

        channel.queueBind(queue.getQueue(), exchange, routingKey);

        return queue;
    }

    /**
     * Returns <tt>false</tt> as this queue is <em>not</em> safe for parallel
     * consumers.
     */
    @Override
    public boolean isParallelConsumable() {
        return false;
    }
}
