package br.com.evollo.kafka;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

@SpringBootApplication
public class KafkaSenderApplication {

	public static void main(String[] args) throws Exception {

		ConfigurableApplicationContext context = SpringApplication.run(KafkaSenderApplication.class, args);

		KafkaMessageProducer producer = context.getBean(KafkaMessageProducer.class);
		MessageListener listener = context.getBean(MessageListener.class);
		/*
		 * Sending a Hello World message to topic 'baeldung'. Must be received by both
		 * listeners with group foo and bar with containerFactory
		 * fooKafkaListenerContainerFactory and barKafkaListenerContainerFactory
		 * respectively. It will also be received by the listener with
		 * headersKafkaListenerContainerFactory as container factory.
		 */
		producer.sendMessage("Hello, World!");
		listener.latch.await(10, TimeUnit.SECONDS);

		/*
		 * Sending message to a topic with 5 partitions, each message to a different
		 * partition. But as per listener configuration, only the messages from
		 * partition 0 and 3 will be consumed.
		 */
		for (int i = 0; i < 5; i++) {
			producer.sendMessageToPartition("Hello To Partitioned Topic!", i);
		}
		listener.partitionLatch.await(10, TimeUnit.SECONDS);

		/*
		 * Sending message to 'filtered' topic. As per listener configuration, all
		 * messages with char sequence 'World' will be discarded.
		 */
		producer.sendMessageToFiltered("Hello Baeldung!");
		producer.sendMessageToFiltered("Hello World!");
		listener.filterLatch.await(10, TimeUnit.SECONDS);

		/*
		 * Sending message to 'greeting' topic. This will send and received a java
		 * object with the help of greetingKafkaListenerContainerFactory.
		 */
		producer.sendGreetingMessage(new Greeting("Greetings", "World!"));
		listener.greetingLatch.await(10, TimeUnit.SECONDS);

		context.close();
	}

	@Bean
	public KafkaMessageProducer messageProducer() {
		return new KafkaMessageProducer();
	}

	@Bean
	public MessageListener messageListener() {
		return new MessageListener();
	}

	public static class MessageListener {

		private CountDownLatch latch = new CountDownLatch(3);

		private CountDownLatch partitionLatch = new CountDownLatch(2);

		private CountDownLatch filterLatch = new CountDownLatch(2);

		private CountDownLatch greetingLatch = new CountDownLatch(1);

		@KafkaListener(topics = "${message.topic.name}", groupId = "foo", containerFactory = "fooKafkaListenerContainerFactory")
		public void listenGroupFoo(String message) {
			System.out.println("Received Message in group 'foo': " + message);
			latch.countDown();
		}

		@KafkaListener(topics = "${message.topic.name}", groupId = "bar", containerFactory = "barKafkaListenerContainerFactory")
		public void listenGroupBar(String message) {
			System.out.println("Received Message in group 'bar': " + message);
			latch.countDown();
		}

		@KafkaListener(topics = "${message.topic.name}", containerFactory = "headersKafkaListenerContainerFactory")
		public void listenWithHeaders(@Payload String message,
				@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
			System.out.println("Received Message: " + message + " from partition: " + partition);
			latch.countDown();
		}

		@KafkaListener(topicPartitions = @TopicPartition(topic = "${partitioned.topic.name}", partitions = { "0",
				"3" }), containerFactory = "partitionsKafkaListenerContainerFactory")
		public void listenToPartition(@Payload String message,
				@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
			System.out.println("Received Message: " + message + " from partition: " + partition);
			this.partitionLatch.countDown();
		}

		@KafkaListener(topics = "${filtered.topic.name}", containerFactory = "filterKafkaListenerContainerFactory")
		public void listenWithFilter(String message) {
			System.out.println("Received Message in filtered listener: " + message);
			this.filterLatch.countDown();
		}

		@KafkaListener(topics = "${greeting.topic.name}", containerFactory = "greetingKafkaListenerContainerFactory")
		public void greetingListener(Greeting greeting) {
			System.out.println("Received greeting message: " + greeting);
			this.greetingLatch.countDown();
		}

	}
}
