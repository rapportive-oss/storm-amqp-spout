package com.rapportive.storm.amqp;

import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.AMQP.Queue;

public class SharedQueueWithBindingTest {
	private Channel channelMock;
	private Queue.DeclareOk queueOkMock;

	@Before
	public void setUp() throws Throwable {
		channelMock = mock(Channel.class);
		queueOkMock = mock(Queue.DeclareOk.class);
		when(channelMock.queueDeclare("queue", true, false, false, null)).thenReturn(queueOkMock);
		when(queueOkMock.getQueue()).thenReturn("queue");
	}

	@Test
	public void declare_WithSampleExchange_DeclaresExchange() throws Throwable {
		SharedQueueWithBinding qBinding = new SharedQueueWithBinding("queue", "sample_exchange", "#");

		qBinding.declare(channelMock);
		verify(channelMock).exchangeDeclarePassive("sample_exchange");
	}

	@Test
	public void declare_WithDefaultExchange_DoesntDeclareExchange() throws Throwable {
		SharedQueueWithBinding qBinding = new SharedQueueWithBinding("queue", "", "#");

		qBinding.declare(channelMock);
		verify(channelMock, never()).exchangeDeclarePassive("");
	}

	@Test
	public void declare_WithSampleExchange_BindsQueueToExchange() throws Throwable {
		SharedQueueWithBinding qBinding = new SharedQueueWithBinding("queue", "sample_exchange", "#");

		qBinding.declare(channelMock);
		verify(channelMock).queueBind("queue", "sample_exchange", "#");
	}

	@Test
	public void declare_WithDefaultExchange_DoesntBindQueueToExchange() throws Throwable {
		SharedQueueWithBinding qBinding = new SharedQueueWithBinding("queue", "", "#");

		qBinding.declare(channelMock);
		verify(channelMock, never()).queueBind("queue", "", "#");
	}

}
