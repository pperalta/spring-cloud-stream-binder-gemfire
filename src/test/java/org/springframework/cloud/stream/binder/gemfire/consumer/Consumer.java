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

package org.springframework.cloud.stream.binder.gemfire.consumer;

import org.springframework.beans.BeansException;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.gemfire.GemfireBinderTests;
import org.springframework.cloud.stream.binder.gemfire.GemfireMessageChannelBinder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.ExecutorSubscribableChannel;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Consumer application that binds a channel to a {@link GemfireMessageChannelBinder}
 * and stores the received message payload.
 */
@RestController
@SpringBootApplication
public class Consumer implements ApplicationRunner, ApplicationContextAware {

	/**
	 * Flag that indicates if the consumer has been bound.
	 */
	private volatile boolean isBound = false;

	/**
	 * Payload of last received message.
	 */
	private volatile String messagePayload;

	private ApplicationContext applicationContext;

	/**
	 * Main method.
	 *
	 * @param args if present, first arg is consumer group name
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		System.out.println("main()");
		SpringApplication.run(Consumer.class, args);
	}

	@Bean
	public GemfireMessageChannelBinder gemfireMessageChannelBinder() throws Exception {
		GemfireMessageChannelBinder binder = new GemfireMessageChannelBinder(GemfireBinderTests.createCache());
		binder.setApplicationContext(this.applicationContext);
		binder.setIntegrationEvaluationContext(new StandardEvaluationContext());
		binder.setBatchSize(1);
		binder.afterPropertiesSet();
		return binder;
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		System.out.println("run()");
		GemfireMessageChannelBinder binder = gemfireMessageChannelBinder();

		SubscribableChannel consumerChannel = new ExecutorSubscribableChannel();
		consumerChannel.subscribe(new MessageHandler() {
			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				messagePayload = (String) message.getPayload();
			}
		});
		String group = null;

		if (args.containsOption("group")) {
			group = args.getOptionValues("group").get(0);
		}

		binder.bindConsumer(GemfireBinderTests.BINDING_NAME, group, consumerChannel, new ConsumerProperties());
		isBound = true;

		Thread.sleep(Long.MAX_VALUE);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@RequestMapping("/is-bound")
	public boolean isBound() {
		return isBound;
	}

	@RequestMapping("/message-payload")
	public String getMessagePayload() {
		return messagePayload;
	}

}
