package com.example.demo;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

public class TestDemoApplication {

	private static final String MESSAGE = Stream
			.generate( ( ) -> "A" )
			.limit( 1 * 1024 * 1024 )
			.collect( Collectors.joining( ) );

	public static void main( String[] args ) {
		final SpringApplication.Running running = SpringApplication
				.from( DemoApplication::main )
				.with( RabbitMQConfiguration.class )
				.run( args );

		final ConfigurableApplicationContext applicationContext = running.getApplicationContext( );
		final RabbitTemplate rabbitTemplate = applicationContext.getBean( RabbitTemplate.class );

		final ExecutorService executorService = Executors.newFixedThreadPool( 2 );

		for ( int i = 0; i < 1000; i++ ) {
			final String messageId = "id-" + i;
			executorService.submit( ( ) -> sendMessage( rabbitTemplate, messageId ) );
		}
	}

	private static void sendMessage( final RabbitTemplate rabbitTemplate, final String messageId ) {
		try {
			final CorrelationData correlationData = new CorrelationData( );
			rabbitTemplate.convertAndSend( Constants.EXCHANGE_NAME, "key", MESSAGE, correlationData );
			System.out.println( "Message " + messageId + " has been sent." );

			final CorrelationData.Confirm confirm = correlationData.getFuture( ).get( 10, TimeUnit.SECONDS );
			System.out.println( "Confirm Ack was " + confirm.isAck( ) );
		} catch ( Exception ex ) {
			throw new RuntimeException( "An exception occured.", ex );
		}
	}

}
