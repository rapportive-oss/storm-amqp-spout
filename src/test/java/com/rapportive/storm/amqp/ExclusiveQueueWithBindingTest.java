package com.rapportive.storm.amqp;

import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;

import com.rabbitmq.client.AMQP.Queue;
import com.rabbitmq.client.Channel;

public class ExclusiveQueueWithBindingTest {
	private Channel channelMock;
	private Queue.DeclareOk queueOkMock;

	@Before
	public void setUp() throws Throwable {
		channelMock = mock(Channel.class);
		queueOkMock = mock(Queue.DeclareOk.class);
		when(channelMock.queueDeclare()).thenReturn(queueOkMock);
		when(queueOkMock.getQueue()).thenReturn("sample_queue");
	}

	@Test
	public void declare_WithSampleExchange_DeclaresExchange() throws Throwable {
		ExclusiveQueueWithBinding qBinding = new ExclusiveQueueWithBinding("sample_exchange", "#");

		qBinding.declare(channelMock);
		verify(channelMock).exchangeDeclarePassive("sample_exchange");
	}

	@Test
	public void declare_WithDefaultExchange_DoesntDeclareExchange() throws Throwable {
		ExclusiveQueueWithBinding qBinding = new ExclusiveQueueWithBinding("", "#");

		qBinding.declare(channelMock);
		verify(channelMock, never()).exchangeDeclarePassive("");
	}

	@Test
	public void declare_WithSampleExchange_BindsQueueToExchange() throws Throwable {
		ExclusiveQueueWithBinding qBinding = new ExclusiveQueueWithBinding("sample_exchange", "#");

		qBinding.declare(channelMock);
		verify(channelMock).queueBind("sample_queue", "sample_exchange", "#");
	}

	@Test
	public void declare_WithDefaultExchange_DoesntBindQueueToExchange() throws Throwable {
		ExclusiveQueueWithBinding qBinding = new ExclusiveQueueWithBinding("", "#");

		qBinding.declare(channelMock);
		verify(channelMock, never()).queueBind("sample_queue", "", "#");
	}

}
