package com.gslab.pepper;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;
import com.google.common.util.concurrent.RateLimiter;
import com.gslab.pepper.exception.PepperBoxException;
import com.gslab.pepper.input.SchemaProcessor;
import com.gslab.pepper.util.ProducerKeys;
import com.gslab.pepper.util.PropsKeys;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import kafka.utils.CommandLineUtils;

public class PepperBoxTransactionalLoadGenerator extends Thread {

	private static Logger log = LogManager.getLogger(PepperBoxLoadGenerator.class.getName());
	private static RateLimiter limiter;
	private Iterator iterator = null;
	private KafkaProducer<String, String> producer;
	private static String topic;
	private static int loopCount;
	private static Properties brokerProps;
	private static Integer sleepBetween;

	/**
	 * Start kafka load generator from input properties and schema
	 *
	 * @param schemaFile
	 * @param producerProps
	 * @param throughput
	 * @param duration
	 * @throws PepperBoxException
	 */
	public PepperBoxTransactionalLoadGenerator(String schemaFile) throws PepperBoxException {
		Path path = Paths.get(schemaFile);
		log.debug("Creating input schema for " + Thread.currentThread().getName());
		try {
			String inputSchema = new String(Files.readAllBytes(path));
			SchemaProcessor schemaProcessor = new SchemaProcessor();

			iterator = schemaProcessor.getPlainTextMessageIterator(inputSchema);
		} catch (IOException e) {
			throw new PepperBoxException(e);
		}
		log.debug("created input schema for " + Thread.currentThread().getName());

		log.debug("Creating producer for" + Thread.currentThread().getName());
		brokerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());
		producer = new KafkaProducer<>(brokerProps);
		log.debug("Created producer for" + Thread.currentThread().getName());
	}

	/**
	 * Retrieve brokers from zookeeper servers
	 *
	 * @param properties
	 * @return
	 */
	private static String getBrokerServers(Properties properties) {

		StringBuilder kafkaBrokers = new StringBuilder();

		String zookeeperServers = properties.getProperty(ProducerKeys.ZOOKEEPER_SERVERS);
		final CountDownLatch connectedSignal = new CountDownLatch(1);
		if (zookeeperServers != null && !zookeeperServers.equalsIgnoreCase(ProducerKeys.ZOOKEEPER_SERVERS_DEFAULT)) {

			try {

				ZooKeeper zk = new ZooKeeper(zookeeperServers, 10000, new Watcher() {

					@Override
					public void process(WatchedEvent event) {
						if (event.getState() == KeeperState.SyncConnected) {
							connectedSignal.countDown();
						}
					}
				});
				connectedSignal.await();
				List<String> ids = zk.getChildren(PropsKeys.BROKER_IDS_ZK_PATH, false);

				for (String id : ids) {

					String brokerInfo = new String(zk.getData(PropsKeys.BROKER_IDS_ZK_PATH + "/" + id, false, null));
					JsonObject jsonObject = Json.parse(brokerInfo).asObject();

					String brokerHost = jsonObject.getString(PropsKeys.HOST, "");
					int brokerPort = jsonObject.getInt(PropsKeys.PORT, -1);

					if (!brokerHost.isEmpty() && brokerPort != -1) {

						kafkaBrokers.append(brokerHost);
						kafkaBrokers.append(":");
						kafkaBrokers.append(brokerPort);
						kafkaBrokers.append(",");

					}

				}
			} catch (IOException | KeeperException | InterruptedException e) {

				log.error("Failed to get broker information", e);

			}

		}

		if (kafkaBrokers.length() > 0) {

			kafkaBrokers.setLength(kafkaBrokers.length() - 1);

			return kafkaBrokers.toString();

		} else {

			return properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);

		}
	}

	@Override
	public void run() {
		log.info("Thread Started : " + Thread.currentThread().getName());
		long sent = 0;
		producer.initTransactions();

		while (sent < loopCount) {
			producer.beginTransaction();
			try {
				boolean success = sendMessage();
				if (success) {
					producer.commitTransaction();
					sent++;
				}
				if (sleepBetween != null)
					Thread.sleep(sleepBetween);
			} catch (InterruptedException e) {
				log.error("Could not send message.", e);
			}
		}
		producer.close();
		log.info("Thread ended : " + Thread.currentThread().getName());
	}

	public boolean sendMessage() throws InterruptedException {
		limiter.acquire();
		ProducerRecord<String, String> keyedMsg = new ProducerRecord<>(topic, iterator.next().toString());

		try {
			producer.send(keyedMsg).get();
			return true;
		} catch (ExecutionException ex) {
			return false;
		}
	}

	public static void checkRequiredArgs(OptionParser parser, OptionSet options, OptionSpec... required) {
		for (OptionSpec optionSpec : required) {
			if (!options.has(optionSpec)) {
				CommandLineUtils.printUsageAndDie(parser, "Missing required argument \"" + optionSpec + "\"");
			}
		}
	}

	public static void main(String[] args) {
		OptionParser parser = new OptionParser();
		ArgumentAcceptingOptionSpec<String> schemaFile = parser.accepts("schema-file", "REQUIRED: Input schema file absolute path.").withRequiredArg()
				.describedAs("schema file").ofType(String.class);
		ArgumentAcceptingOptionSpec<String> producerConfig = parser.accepts("producer-config-file", "REQUIRED: Kafka producer properties file absolute path.")
				.withRequiredArg().describedAs("producer properties").ofType(String.class);
		ArgumentAcceptingOptionSpec<Integer> throughput = parser.accepts("throughput-per-producer", ": Throttle rate per thread.").withOptionalArg()
				.describedAs("throughput").ofType(Integer.class);
		ArgumentAcceptingOptionSpec<Integer> loopCount = parser.accepts("loop-count", "REQUIRED: Test count .").withRequiredArg().describedAs("test duration")
				.ofType(Integer.class);
		ArgumentAcceptingOptionSpec<Integer> threadCount = parser.accepts("num-producers", "REQUIRED: Number of producer threads.").withRequiredArg()
				.describedAs("producers").ofType(Integer.class);
		ArgumentAcceptingOptionSpec<Integer> sleepBetween = parser.accepts("sleep-between", "sleep between loops.").withOptionalArg().describedAs("producers")
				.ofType(Integer.class);
		if (args.length == 0) {
			CommandLineUtils.printUsageAndDie(parser, "Kafka console load generator.");
		}
		OptionSet options = parser.parse(args);
		checkRequiredArgs(parser, options, schemaFile, producerConfig, loopCount, threadCount);
		try {
			fillBrokerProps(options.valueOf(producerConfig), options.valueOf(throughput), options.valueOf(loopCount), options.valueOf(sleepBetween));
		} catch (PepperBoxException e) {
			log.error("Failed to fill properties", e);
		}
		try {
			int totalThreads = options.valueOf(threadCount);
			for (int i = 0; i < totalThreads; i++) {
				PepperBoxTransactionalLoadGenerator jsonProducer = new PepperBoxTransactionalLoadGenerator(options.valueOf(schemaFile));
				jsonProducer.start();
				Thread.sleep(100);
			}

		} catch (Exception e) {
			log.error("Failed to generate load", e);
		}
	}

	public static void fillBrokerProps(String producerProps, Integer throughput, Integer lCount, Integer sleepBetween) throws PepperBoxException {
		log.debug("Filling input props  for " + Thread.currentThread().getName());
		Properties inputProps = new Properties();
		try {
			inputProps.load(new FileInputStream(producerProps));
		} catch (IOException e) {
			throw new PepperBoxException(e);
		}
		if (throughput != null) {
			limiter = RateLimiter.create(throughput);
		} else {
			limiter = RateLimiter.create(1000);
		}

		loopCount = lCount;
		if (sleepBetween != null)
			PepperBoxTransactionalLoadGenerator.sleepBetween = sleepBetween;
		brokerProps = new Properties();
		brokerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokerServers(inputProps));
		brokerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, inputProps.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
		brokerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, inputProps.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
		brokerProps.put(ProducerConfig.ACKS_CONFIG, inputProps.getProperty(ProducerConfig.ACKS_CONFIG));
		brokerProps.put(ProducerConfig.SEND_BUFFER_CONFIG, inputProps.getProperty(ProducerConfig.SEND_BUFFER_CONFIG));
		brokerProps.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, inputProps.getProperty(ProducerConfig.RECEIVE_BUFFER_CONFIG));
		brokerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, inputProps.getProperty(ProducerConfig.BATCH_SIZE_CONFIG));
		brokerProps.put(ProducerConfig.LINGER_MS_CONFIG, inputProps.getProperty(ProducerConfig.LINGER_MS_CONFIG));
		brokerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, inputProps.getProperty(ProducerConfig.BUFFER_MEMORY_CONFIG));
		brokerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		brokerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, inputProps.getProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG));
		String kerbsEnabled = inputProps.getProperty(ProducerKeys.KERBEROS_ENABLED);

		if (kerbsEnabled != null && kerbsEnabled.equals(ProducerKeys.FLAG_YES)) {

			System.setProperty(ProducerKeys.JAVA_SEC_AUTH_LOGIN_CONFIG, inputProps.getProperty(ProducerKeys.JAVA_SEC_AUTH_LOGIN_CONFIG));
			System.setProperty(ProducerKeys.JAVA_SEC_KRB5_CONFIG, inputProps.getProperty(ProducerKeys.JAVA_SEC_KRB5_CONFIG));
			brokerProps.put(ProducerKeys.SASL_KERBEROS_SERVICE_NAME, inputProps.getProperty(ProducerKeys.SASL_KERBEROS_SERVICE_NAME));
			brokerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, inputProps.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
		}
		log.debug("Filling input props  for " + Thread.currentThread().getName());
		log.debug("Filling parmeters  for " + Thread.currentThread().getName());
		Set<String> parameters = inputProps.stringPropertyNames();
		parameters.forEach(parameter -> {
			if (parameter.startsWith("_")) {
				brokerProps.put(parameter.substring(1), inputProps.getProperty(parameter));
			}
		});
		log.debug("Filled parmeters  for " + Thread.currentThread().getName());

		topic = inputProps.getProperty(ProducerKeys.KAFKA_TOPIC_CONFIG);

	}
}
