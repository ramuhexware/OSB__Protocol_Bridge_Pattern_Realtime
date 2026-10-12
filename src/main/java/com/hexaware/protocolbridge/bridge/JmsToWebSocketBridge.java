package com.hexaware.protocolbridge.bridge;

import jakarta.jms.Message;
import jakarta.jms.TextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

/**
 * Event-Driven Asynchronous Protocol Bridge (JMS Topic -> WebSocket / STOMP).
 * Bridges enterprise backend broadcast messages onto real-time WebSocket topics
 * consumed by web and mobile client frontends.
 */
@Component
public class JmsToWebSocketBridge {

    private static final Logger log = LoggerFactory.getLogger(JmsToWebSocketBridge.class);

    private final SimpMessagingTemplate webSocketTemplate;

    public JmsToWebSocketBridge(SimpMessagingTemplate webSocketTemplate) {
        this.webSocketTemplate = webSocketTemplate;
    }

    /**
     * Listens on enterprise JMS Notification Topic and bridges onto WebSocket STOMP
     */
    @JmsListener(
            destination = "${bridge.notification-topic:freddie.lpa.notifications.topic}",
            containerFactory = "topicListenerFactory"
    )
    public void onEnterpriseTopicEvent(Message message) {
        try {
            String payload;
            if (message instanceof TextMessage textMessage) {
                payload = textMessage.getText();
            } else {
                payload = message.toString();
            }

            String correlationId = message.getJMSCorrelationID();
            String eventType = message.getStringProperty("EVENT_TYPE");

            log.info("Bridging JMS Topic Event to WebSocket: correlationId={}, type={}", correlationId, eventType);

            // Forward onto WebSocket destination /topic/live-events
            webSocketTemplate.convertAndSend("/topic/live-events", payload);

        } catch (Exception e) {
            log.error("Error in JMS to WebSocket protocol bridge", e);
        }
    }
}
