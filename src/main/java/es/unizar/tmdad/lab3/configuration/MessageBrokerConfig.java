package es.unizar.tmdad.lab3.configuration;

import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@EnableRabbit
@PropertySource(value = { "classpath:messagebroker.properties" })
public class MessageBrokerConfig {
	
	@Value("${rabbitmq.host}")
	private String host;
	
	@Value("${rabbitmq.user}")
	private String user;
	
	@Value("${rabbitmq.pw}")
	private String pw;
	
	@Value("${rabbitmq.vhost}")
	private String vhost;
	
	@Bean
	public MessageConverter jsonMessageConverter(){
		return new Jackson2JsonMessageConverter();
	}
	
	@Bean
	public CachingConnectionFactory cachingConnectionFactory(){
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory(host);
		connectionFactory.setUsername(user);
		connectionFactory.setPassword(pw);
		connectionFactory.setVirtualHost(vhost);
		
		connectionFactory.setRequestedHeartBeat(30);
		connectionFactory.setConnectionTimeout(30000);
		
		return connectionFactory;
	}
	
	@Bean
	public RabbitAdmin rabbitAdmin(){
		CachingConnectionFactory connectionFactory = cachingConnectionFactory();
		return new RabbitAdmin(connectionFactory);
	}
	
	@Bean
	public RabbitTemplate rabbitTemplate(){
		CachingConnectionFactory connectionFactory = cachingConnectionFactory();
		
		RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
		rabbitTemplate.setMessageConverter(jsonMessageConverter());
		
		return rabbitTemplate;
	}
}