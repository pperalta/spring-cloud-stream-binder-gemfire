/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.gemfire;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.LocatorLauncher;
import com.oracle.tools.runtime.LocalPlatform;
import com.oracle.tools.runtime.PropertiesBuilder;
import com.oracle.tools.runtime.console.SystemApplicationConsole;
import com.oracle.tools.runtime.java.JavaApplication;
import com.oracle.tools.runtime.java.LocalJavaApplicationBuilder;
import com.oracle.tools.runtime.java.SimpleJavaApplication;
import com.oracle.tools.runtime.java.SimpleJavaApplicationSchema;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.cloud.stream.binder.gemfire.consumer.Consumer;
import org.springframework.cloud.stream.binder.gemfire.producer.Producer;
import org.springframework.data.gemfire.CacheFactoryBean;
import org.springframework.integration.test.util.SocketUtils;
import org.springframework.web.client.RestTemplate;

/**
 * Tests for {@link GemfireMessageChannelBinder}.
 *
 * @author Patrick Peralta
 */
public class GemfireBinderTests {
	private static final Logger logger = LoggerFactory.getLogger(GemfireBinderTests.class);

	/**
	 * Timeout value in milliseconds for operations to complete.
	 */
	public static final long TIMEOUT = 30000;

	/**
	 * Payload of test message.
	 */
	public static final String MESSAGE_PAYLOAD = "hello world";

	/**
	 * Name of binding used for producer and consumer bindings.
	 */
	public static final String BINDING_NAME = "test";

	/**
	 * Name of GemFire Locator.
	 */
	public static final String LOCATOR_NAME = "locator1";


	/**
	 * Test basic message sending functionality.
	 *
	 * @throws Exception
	 */
	@Test
	public void testMessageSendReceive() throws Exception {
		testMessageSendReceive(null, false);
	}

	/**
	 * Test usage of partition selector.
	 *
	 * @throws Exception
	 */
	@Test
	public void testPartitionedMessageSendReceive() throws Exception {
		testMessageSendReceive(null, true);
	}

	/**
	 * Test consumer group functionality.
	 *
	 * @throws Exception
	 */
	@Test
	public void testMessageSendReceiveConsumerGroups() throws Exception {
		testMessageSendReceive(new String[]{"a", "b"}, false);
	}

	/**
	 * Test message sending functionality.
	 *
	 * @param groups consumer groups; may be {@code null}
	 * @throws Exception
	 */
	private void testMessageSendReceive(String[] groups, boolean partitioned) throws Exception {
		LocatorLauncher locatorLauncher = null;
		Map<Integer, JavaApplication> consumers = new HashMap<>();
		Map<Integer, JavaApplication> producers = new HashMap<>();
		int locatorPort = SocketUtils.findAvailableServerSocket();

		try {
			locatorLauncher = new LocatorLauncher.Builder()
					.setMemberName(LOCATOR_NAME)
					.setPort(locatorPort)
					.setRedirectOutput(true)
					.build();

			locatorLauncher.start();
			locatorLauncher.waitOnStatusResponse(TIMEOUT, 5, TimeUnit.MILLISECONDS);

			Properties moduleProperties = new Properties();
			moduleProperties.setProperty("gemfire.locators", String.format("localhost[%d]", locatorPort));
			if (partitioned) {
				moduleProperties.setProperty("partitioned", "true");
			}

			int consumerCount = groups == null ? 1 : groups.length;

			for (int i = 0; i < consumerCount; i++) {
				int consumerPort = SocketUtils.findAvailableServerSocket();
				List<String> args = new ArrayList<>();
				args.add(String.format("--server.port=%d", consumerPort));
				if (groups != null) {
					args.add(String.format("--group=%s", groups[i]));
				}
				consumers.put(consumerPort, launch(Consumer.class, moduleProperties, args));
			}
			for (int consumerPort : consumers.keySet()) {
				waitForConsumer(consumerPort);
			}

			int producerPort = SocketUtils.findAvailableServerSocket();
			producers.put(producerPort, launch(Producer.class, moduleProperties,
					Collections.singletonList(String.format("--server.port=%d", producerPort))));

			for (int consumerPort : consumers.keySet()) {
				assertEquals(MESSAGE_PAYLOAD, waitForMessage(consumerPort));
			}

			if (partitioned) {
				assertTrue(partitionSelectorUsed(producers.keySet().iterator().next()));
			}
		}
 		finally {
			for (JavaApplication producer : producers.values()) {
					producer.close();
				}
				for (JavaApplication consumer : consumers.values()) {
					consumer.close();
				}
			if (locatorLauncher != null) {
				locatorLauncher.stop();
			}
			cleanLocatorFiles(locatorPort);
		}
	}

	/**
	 * Remove the files generated by the GemFire Locator.
	 */
	private void cleanLocatorFiles(int port) {
		Path workingPath = Paths.get(".");
		deleteFile(workingPath.resolve(String.format("ConfigDiskDir_%s", LOCATOR_NAME)).toFile());
		deleteFile(workingPath.resolve(String.format("%s.log", LOCATOR_NAME)).toFile());
		deleteFile(workingPath.resolve(String.format("locator%dstate.dat", port)).toFile());
		deleteFile(workingPath.resolve(String.format("locator%dviews.log", port)).toFile());
		deleteFile(workingPath.resolve("BACKUPDEFAULT.if").toFile());
		deleteFile(workingPath.resolve("DRLK_IFDEFAULT.lk").toFile());
	}

	/**
	 * Deletes the file or directory denoted by the given {@link File}.
	 * If this {@code File} denotes a directory, then the files contained
	 * in the directory will be recursively deleted.
	 *
	 * @param file the file to be deleted
	 */
	private void deleteFile(File file) {
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			if (files == null) {
				logger.warn("Could not delete directory {}", file);
			}
			else {
				for (File f : files) {
					deleteFile(f);
				}
			}
		}
		if (!file.setWritable(true)) {
			logger.warn("Could not set write permissions on {}", file);
		}
		if (!file.delete()) {
			logger.warn("Could not delete {}", file);
		}
	}

	/**
	 * Block the executing thread until the consumer is bound.
	 *
	 * @param consumer the consumer application
	 * @throws InterruptedException if the thread is interrupted
	 * @throws AssertionError if the consumer is not bound after
	 * {@value #TIMEOUT} milliseconds
	 */
	private void waitForConsumer(int port) throws InterruptedException {
		long start = System.currentTimeMillis();
		while (System.currentTimeMillis() < start + TIMEOUT) {
			if (isConsumerBound(port)) {
				return;
			}
			else {
				Thread.sleep(1000);
			}
		}
		assertTrue("Consumer not bound", isConsumerBound(port));
	}

	private boolean isConsumerBound(int port) {
		RestTemplate template = new RestTemplate();
		return template.getForObject(String.format("http://localhost:%d/is-bound", port), Boolean.class);
	}

	private String getConsumerMessagePayload(int port) {
		RestTemplate template = new RestTemplate();
		return template.getForObject(String.format("http://localhost:%d/message-payload", port), String.class);
	}

	private boolean partitionSelectorUsed(int port) throws InterruptedException {
		RestTemplate template = new RestTemplate();
		return template.getForObject(String.format("http://localhost:%d/partition-strategy-invoked", port), Boolean.class);
	}

	/**
	 * Block the executing thread until a message is received by the
	 * consumer application, or until {@value #TIMEOUT} milliseconds elapses.
	 *
	 * @param consumer the consumer application
	 * @return the message payload that was received
	 * @throws InterruptedException if the thread is interrupted
	 */
	private String waitForMessage(int port) throws InterruptedException {
		long start = System.currentTimeMillis();
		String message = null;
		while (System.currentTimeMillis() < start + TIMEOUT) {
			message = getConsumerMessagePayload(port);
			if (message == null) {
				Thread.sleep(1000);
			}
			else {
				break;
			}
		}
		return message;
	}

	/**
	 * Launch the given class's {@code main} method in a separate JVM.
	 *
	 * @param clz               class to launch
	 * @param systemProperties  system properties for new process
	 * @param args              command line arguments
	 * @return launched application
	 *
	 * @throws IOException if an exception was thrown launching the process
	 */
	private JavaApplication launch(Class<?> clz, Properties systemProperties,
			List<String> args) throws IOException {
		String classpath = System.getProperty("java.class.path");

		logger.info("Launching {}", clz);
		logger.info("	args: {}", args);
		logger.info("	properties: {}", systemProperties);
		logger.info("	classpath: {}", classpath);

		SimpleJavaApplicationSchema schema = new SimpleJavaApplicationSchema(clz.getName(), classpath);
		if (args != null) {
			for (String arg : args) {
				schema.addArgument(arg);
			}
		}
		if (systemProperties != null) {
			schema.setSystemProperties(new PropertiesBuilder(systemProperties));
		}
		schema.setWorkingDirectory(Files.createTempDirectory(null).toFile());

		LocalJavaApplicationBuilder<SimpleJavaApplication> builder =
				new LocalJavaApplicationBuilder<>(LocalPlatform.getInstance());
		return builder.realize(schema, clz.getName(), new SystemApplicationConsole());
	}

	/**
	 * Create a {@link Cache} with hard coded properties for testing.
	 *
	 * @return Cache for testing
	 *
	 * @throws Exception
	 */
	public static Cache createCache() throws Exception {
		CacheFactoryBean bean = new CacheFactoryBean();
		Properties properties = new Properties();
		properties.put("locators", "localhost[7777]");
		properties.put("log-level", "warning");
		properties.put("mcast-port", "0");
		bean.setProperties(properties);

		return bean.getObject();
	}

//	public static class ConsumerBoundChecker implements RemoteCallable<Boolean> {
//		@Override
//		public Boolean call() throws Exception {
//			return Consumer.isBound;
//		}
//	}
//
//	public static class ConsumerMessageExtractor implements RemoteCallable<String> {
//		@Override
//		public String call() throws Exception {
//			return Consumer.messagePayload;
//		}
//	}
//
//	public static class ProducerPartitionSelectorChecker implements RemoteCallable<Boolean> {
//		@Override
//		public Boolean call() throws Exception {
//			return Producer.partitionSelectorStrategy.invoked;
//		}
//	}

}
