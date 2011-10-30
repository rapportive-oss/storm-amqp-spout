package com.rapportive.storm.amqp;

import java.io.IOException;
import java.io.Serializable;

import com.rabbitmq.client.AMQP.Queue;

import com.rabbitmq.client.Channel;

/**
 * Declaration of a queue to consume, and any exchange bindings the queue needs.
 *
 * <p>Depending on the queue parameters (exclusive, auto_delete, server-named)
 * and exchange bindings, it may or may not be safe to start several consumers
 * in parallel using a given queue declaration.  For example, an exclusive
 * named queue bound to an exchange is not safe because only one of the
 * consumers will succeed in declaring the queue; an exclusive
 * <em>server-named</em> queue does not have that problem, but is still
 * probably not safe, because most exchange types will send a copy of every
 * message to every queue bound to them, so you will end up consuming each
 * message several times.</p>
 *
 * <p>For that reason, to implement this interface you must implement
 * {@link #isParallelConsumable} to indicate whether or not this queue is safe
 * for parallel consumers.</p>
 */
public interface QueueDeclaration extends Serializable {
    /**
     * Declare the queue, and any exchanges and bindings that it needs.  Called
     * once to determine the queue to consume from.
     *
     * @param channel  An open AMQP channel which can be used to send the
     *                 declarations.
     *
     * @return the server's response to the successful queue declaration (used
     *         to determine the queue name to subscribe to).
     *
     * @throws IOException if a declaration fails or the AMQP connection drops.
     */
    Queue.DeclareOk declare(Channel channel) throws IOException;

    /**
     * Indicate whether this queue is safe for parallel consumers.
     *
     * @return <tt>true</tt> if safe for parallel consumers, otherwise
     *         <tt>false</tt>.
     */
    boolean isParallelConsumable();
}
