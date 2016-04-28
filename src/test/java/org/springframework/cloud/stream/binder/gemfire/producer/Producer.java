/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cloud.stream.binder.gemfire.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.BeansException;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.binder.PartitionSelectorStrategy;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.gemfire.GemfireBinderTests;
import org.springframework.cloud.stream.binder.gemfire.GemfireMessageChannelBinder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.ExecutorSubscribableChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Producer application that binds a channel to a {@link GemfireMessageChannelBinder}
 * and sends a test message.
 */
@RestController
@SpringBootApplication
public class Producer implements ApplicationRunner, ApplicationContextAware {

	private static final Logger logger = LoggerFactory.getLogger(Producer.class);

	private ApplicationContext applicationContext;

	public static void main(String[] args) {
		SpringApplication.run(Producer.class);
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		GemfireMessageChannelBinder binder = gemfireMessageChannelBinder();
		SubscribableChannel producerChannel = producerChannel();

		ProducerProperties properties = new ProducerProperties();
		properties.setPartitionKeyExpression(new SpelExpressionParser().parseExpression("payload"));
		binder.bindProducer(GemfireBinderTests.BINDING_NAME, producerChannel, properties);

		Message<String> message = new GenericMessage<>(GemfireBinderTests.MESSAGE_PAYLOAD);
		producerChannel.send(message);

		Thread.sleep(Long.MAX_VALUE);
	}

	@Bean
	public SubscribableChannel producerChannel() {
		return new ExecutorSubscribableChannel();
	}

	@Bean
	public StubPartitionSelectorStrategy stubPartitionSelectorStrategy() {
		return new StubPartitionSelectorStrategy();
	}

	@Bean
	public GemfireMessageChannelBinder gemfireMessageChannelBinder() throws Exception {
		GemfireMessageChannelBinder binder = new GemfireMessageChannelBinder(GemfireBinderTests.createCache());
		binder.setApplicationContext(this.applicationContext);
		binder.setIntegrationEvaluationContext(new StandardEvaluationContext());
		// todo
		if (Boolean.getBoolean("partitioned")) {
			System.out.println("setting partition selector");
			binder.setPartitionSelector(stubPartitionSelectorStrategy());
		}
		binder.afterPropertiesSet();
		return binder;
	}

	@RequestMapping("/partition-strategy-invoked")
	public boolean partitionStrategyInvoked() {
		return stubPartitionSelectorStrategy().invoked;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}


	public static class StubPartitionSelectorStrategy implements PartitionSelectorStrategy {
		public volatile boolean invoked = false;

		@Override
		public int selectPartition(Object key, int partitionCount) {
			logger.warn("Selecting partition for key {}; partition count: {}", key, partitionCount);
			System.out.printf("Selecting partition for key %s; partition count: %d", key, partitionCount);
			invoked = true;
			return 1;
		}
	}

}
