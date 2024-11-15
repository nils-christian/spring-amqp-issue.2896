package com.example.demo;

import org.springframework.amqp.core.TopicExchange;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

@TestConfiguration(proxyBeanMethods = false)
class RabbitMQConfiguration {

	@Bean
	@ServiceConnection
	RabbitMQContainer rabbitContainer() {
		return new RabbitMQContainer(DockerImageName.parse("rabbitmq:latest"));
	}

	@Bean
	TopicExchange testTopicExchange( ) {
		return new TopicExchange( Constants.EXCHANGE_NAME );
	}

}
