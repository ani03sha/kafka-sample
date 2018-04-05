package org.anirudh.redquark.kafkasample.simple;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 
 * Producer class that creates messages based on a topic
 *
 */
public class Producer {

	/**
	 * Scanner variable to read message from the console
	 */
	private static Scanner in;

	public static void main(String[] args) {

		/**
		 * If block to check if at least something is passed from the console. Exit if
		 * isn't.
		 */
		if (args.length != 1) {
			System.err.println("Please specify at lease one parameter");
			System.exit(-1);
		}

		/**
		 * Getting the topic name. This will be the first parameter in the console
		 */
		String topicName = args[0];

		/**
		 * Initializing the Scanner instance
		 */
		in = new Scanner(System.in);

		System.out.println("Enter message (type exit to quit)");

		/**
		 * Configuring the producer. We configure the producer by creating an object
		 * from the java.util.Properties class and setting its properties. The
		 * ProducerConfig class defines all the different properties available, but
		 * Kafka's default values are sufficient for most uses.
		 */
		Properties configProperties = new Properties();

		/**
		 * BOOTSTRAP_SERVERS_CONFIG (bootstrap.servers) sets a list of host:port pairs
		 * used for establishing the initial connections to the Kakfa cluster in the
		 * host1:port1,host2:port2,... format. Even if we have more than one broker in
		 * our Kafka cluster, we only need to specify the value of the first broker's
		 * host:port. The Kafka client will use this value to make a discover call on
		 * the broker, which will return a list of all the brokers in the cluster. It's
		 * a good idea to specify more than one broker in the BOOTSTRAP_SERVERS_CONFIG,
		 * so that if that first broker is down the client will be able to try other
		 * brokers.
		 * 
		 */
		configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		/**
		 * To configure the message key, we set a value of KEY_SERIALIZER_CLASS_CONFIG
		 * on the org.apache.kafka.common.serialization.ByteArraySerializer. This works
		 * because null doesn't need to be converted into byte[].
		 */
		configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.ByteArraySerializer");

		/**
		 * For the message value, we set VALUE_SERIALIZER_CLASS_CONFIG on the
		 * org.apache.kafka.common.serialization.StringSerializer, because that class
		 * knows how to convert a String into a byte[].
		 */
		configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");

		/**
		 * Creating a KafkaProducer. This is the Kafka client which publishes messages
		 * to the Kafka cluster
		 */
		org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<String, String>(
				configProperties);
		
		/**
		 * Reading input from the console
		 */
		String line = in.nextLine();
		
		/**
		 * Loop until exit command occurs
		 */
		while(!line.equals("exit")){
			
			/**
			 * Creating a record
			 */
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, line);
			
			/**
			 * Sending the record to the cluster
			 */
			producer.send(record);
			
			/**
			 * Reading the next input
			 */
			line = in.nextLine();
		}
		
		/**
		 * Closing the Scanner instance
		 */
		in.close();
		
		/**
		 * Closing the Producer connection
		 */
		producer.close();
	}
}
