package org.anirudh.redquark.kafkasample.simple;

import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * A simple consumer that subscribes to a topic. Whenever a new message is
 * published to the topic, it will read that message and print it to the
 * console.
 *
 */
public class Consumer {

	/**
	 * Scanner variable to read message from the console
	 */
	private static Scanner in;

	public static void main(String[] args) {

		/**
		 * Check if at lease two arguments are passed. They should be topic name and
		 * group id
		 */
		if (args.length != 2) {
			System.err.printf("Usage: %s <topicName> <groupId>\n", Consumer.class.getSimpleName());
			System.exit(-1);
		}

		/**
		 * Initializing Scanner instance
		 */
		in = new Scanner(System.in);

		/**
		 * Getting the topic name
		 */
		String topicName = args[0];

		/**
		 * Getting the group id
		 */
		String groupId = args[1];

		/**
		 * Initializing consumer thread
		 */
		ConsumerThread consumerThread = new ConsumerThread(topicName, groupId);

		/**
		 * Starting the thread. The ConsumerThead starts an infinite loop and keeps
		 * polling the topic for new messages
		 */
		consumerThread.start();

		String line = "";

		/**
		 * Iterating until the exit command occurs
		 */
		while (!line.equals("exit")) {
			line = in.next();
		}

		/**
		 * Meanwhile in the Consumer class, the main thread waits for a user to enter
		 * exit on the console. Once a user enters exit, it calls the
		 * KafkaConsumer.wakeup() method, causing the KafkaConsumer to stop polling for
		 * new messages and throw a WakeupException.
		 */
		consumerThread.getKafkaConsumer().wakeup();
		
		System.out.println("Stopping consumer...");
		
		/**
		 * Give CPU to main thread after consumerThread done running
		 */
		try {
			consumerThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	/**
	 * ConsumerThread is an inner class that takes a topic name and group name as
	 * its arguments. In the run() method it creates a KafkaConsumer object, with
	 * appropriate properties. It subscribes to the topic that was passed as an
	 * argument in the constructor, by calling the kafkaConsumer.subscribe() method,
	 * then polls the Kafka server every 100 milliseconds to check if there are any
	 * new messages in the topic. It will iterate through the list of any new
	 * messages and print them to the console.
	 */
	private static class ConsumerThread extends Thread {

		/**
		 * Topic passed
		 */
		private String topicName;

		/**
		 * Group ID passed
		 */
		private String groupId;

		/**
		 * KafkaConsumer instance
		 */
		private KafkaConsumer<String, String> kafkaConsumer;

		/**
		 * Parameterized constructor
		 */
		public ConsumerThread(String topicName, String groupId) {
			super();
			this.topicName = topicName;
			this.groupId = groupId;
		}

		@Override
		public void run() {

			/**
			 * Configuring the Consumer. We configure the consumer by creating an object
			 * from the java.util.Properties class and setting its properties. The
			 * ConsumerConfig class defines all the different properties available, but
			 * Kafka's default values are sufficient for most uses.
			 */
			Properties configProperties = new Properties();

			/**
			 * BOOTSTRAP_SERVERS_CONFIG to configure the host/port pairs for the consumer
			 * class. This config lets us establish the initial connections to the Kakfa
			 * cluster in the host1:port1,host2:port2,... format.
			 */
			configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

			/**
			 * Here we deserialize the key and value according to the type we set in the
			 * producer
			 */
			configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
					"org.apache.kafka.common.serialization.ByteArrayDeserializer");
			configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
					"org.apache.kafka.common.serialization.StringDeserializer");

			/**
			 * We need to set the value of the GROUP_ID_CONFIG. This should be a group name
			 * in string format
			 */
			configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

			/**
			 * Client Id config
			 */
			configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");

			/**
			 * Initializing Kafka consumer
			 */
			kafkaConsumer = new KafkaConsumer<>(configProperties);

			/**
			 * Subacribe to topic
			 */
			kafkaConsumer.subscribe(Arrays.asList(topicName));

			/**
			 * Start processing messages
			 */
			try {
				while (true) {
					/**
					 * Polling the Kafka server ever 100 milliseconds
					 */
					ConsumerRecords<String, String> records = kafkaConsumer.poll(100);

					/**
					 * Printing value of each record
					 */
					for (ConsumerRecord<String, String> record : records) {
						System.out.println(record.value());
					}
				}
			} catch (Exception ex) {
				System.out.println("Exception caught " + ex.getMessage());
			} finally {
				kafkaConsumer.close();
				System.out.println("After closing KafkaConsumer");
			}
		}

		public KafkaConsumer<String, String> getKafkaConsumer() {
			return this.kafkaConsumer;
		}
	}
}
