package com.hexaware.protocolbridge.config;

import jakarta.jms.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jms.DefaultJmsListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerContainerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.converter.MappingJackson2MessageConverter;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.jms.support.converter.MessageType;

import java.util.HashMap;
import java.util.Map;

/**
 * JMS Infrastructure Configuration for OSB Protocol Bridge Pattern.
 * Configures connection factories, message converters, and concurrency listeners.
 */
@Configuration
@EnableJms
public class JmsConfig {

    @Value("${bridge.request-queue:freddie.lpa.loan.request.queue}")
    private String requestQueue;

    @Value("${bridge.reply-queue:freddie.lpa.loan.reply.queue}")
    private String replyQueue;

    @Value("${bridge.notification-topic:freddie.lpa.notifications.topic}")
    private String notificationTopic;

    @Bean
    public MessageConverter jacksonJmsMessageConverter(com.fasterxml.jackson.databind.ObjectMapper objectMapper) {
        MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
        converter.setObjectMapper(objectMapper);
        converter.setTargetType(MessageType.TEXT);
        converter.setTypeIdPropertyName("_type");

        // Type mappings for Jackson JMS converter
        Map<String, Class<?>> typeIdMappings = new HashMap<>();
        typeIdMappings.put("CanonicalLoanMessage", com.hexaware.protocolbridge.model.CanonicalLoanMessage.class);
        typeIdMappings.put("CanonicalUnderwritingDecision", com.hexaware.protocolbridge.model.CanonicalUnderwritingDecision.class);
        converter.setTypeIdMappings(typeIdMappings);

        return converter;
    }

    @Bean
    public JmsTemplate jmsTemplate(ConnectionFactory connectionFactory, MessageConverter jacksonJmsMessageConverter) {
        JmsTemplate template = new JmsTemplate(connectionFactory);
        template.setMessageConverter(jacksonJmsMessageConverter);
        template.setReceiveTimeout(6000L); // 6s SLA timeout
        template.setDeliveryPersistent(false); // fast in-memory
        template.setPubSubDomain(false);
        return template;
    }

    @Bean
    public JmsTemplate topicJmsTemplate(ConnectionFactory connectionFactory, MessageConverter jacksonJmsMessageConverter) {
        JmsTemplate template = new JmsTemplate(connectionFactory);
        template.setMessageConverter(jacksonJmsMessageConverter);
        template.setDeliveryPersistent(false);
        template.setPubSubDomain(true); // PubSub MULTICAST for Topics
        return template;
    }

    /**
     * Listener container factory for queue point-to-point listeners (AUS Engine and Bridge Reply Listener)
     */
    @Bean
    public JmsListenerContainerFactory<?> queueListenerFactory(
            ConnectionFactory connectionFactory,
            DefaultJmsListenerContainerFactoryConfigurer configurer,
            MessageConverter jacksonJmsMessageConverter) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        configurer.configure(factory, connectionFactory);
        factory.setMessageConverter(jacksonJmsMessageConverter);
        factory.setConcurrency("3-10"); // concurrent consumer pool
        factory.setPubSubDomain(false);
        return factory;
    }

    /**
     * Listener container factory for publish/subscribe topic listeners (Realtime WebSocket Bridge)
     */
    @Bean
    public JmsListenerContainerFactory<?> topicListenerFactory(
            ConnectionFactory connectionFactory,
            DefaultJmsListenerContainerFactoryConfigurer configurer,
            MessageConverter jacksonJmsMessageConverter) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        configurer.configure(factory, connectionFactory);
        factory.setMessageConverter(jacksonJmsMessageConverter);
        factory.setPubSubDomain(true); // Topic subscription
        return factory;
    }
}
