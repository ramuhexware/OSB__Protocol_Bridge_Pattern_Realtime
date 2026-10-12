package com.hexaware.protocolbridge.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

/**
 * WebSocket and STOMP Protocol Configuration.
 * Used for real-time outbound protocol bridging to browser and mobile dashboards.
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) {
        // In-memory message broker prefix for topic subscriptions
        config.enableSimpleBroker("/topic");
        // Application prefix for messages originating from clients
        config.setApplicationDestinationPrefixes("/app");
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        // Register WebSocket handshake endpoint with SockJS fallback support
        registry.addEndpoint("/ws-bridge")
                .setAllowedOriginPatterns("*");
        registry.addEndpoint("/ws-bridge")
                .setAllowedOriginPatterns("*")
                .withSockJS();
    }
}
