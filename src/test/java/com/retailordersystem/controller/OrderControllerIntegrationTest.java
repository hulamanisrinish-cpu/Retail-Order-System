package com.retailordersystem.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.test.web.servlet.MockMvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.retailordersystem.event.OrderPlacedEvent;
import com.retailordersystem.model.Order;
import com.retailordersystem.repository.OrderRepository;

@SpringBootTest
@AutoConfigureMockMvc
public class OrderControllerIntegrationTest {
	private static final Logger logger = LoggerFactory.getLogger(OrderControllerIntegrationTest.class);
	

	@Value("${spring.kafka.bootstrap-servers}")
	private String kafkaBootstrapServers;
	
	@Autowired
    private MockMvc mockMvc;

    @Autowired
    private OrderRepository orderRepository;
    
    @Autowired
    private ConsumerFactory<String, OrderPlacedEvent> consumerFactory;
    

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setup() {
        // Clean up table first
        orderRepository.deleteAll();
    }

    @AfterEach
    void tearDown() {
        orderRepository.deleteAll();
    }

    
    @Test
    void testCreateOrder() throws Exception {
        // Given: Order request payload
        Order order = new Order();
        order.setStatus("NEW");
        order.setDescription("Test Description");

        String orderJson = objectMapper.writeValueAsString(order);

        // When & Then: Call API and verify response
        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(orderJson))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("NEW"));

        // Verify Order saved in DB
        List<Order> orders = orderRepository.findAll();
        assertThat(orders).hasSize(1);
        assertThat(orders.get(0).getDescription()).isEqualTo("Test Description");

        // Verify Kafka event consumption using Awaitility
        await().atMost(5, TimeUnit.SECONDS) // Adjust timeout as needed
                .pollInterval(500, TimeUnit.MILLISECONDS) // Adjust polling interval
                .untilAsserted(() -> {
                    OrderPlacedEvent receivedEvent = consumeKafkaEvent("order_placed_topic");
                    assertThat(receivedEvent).isNotNull();
                    assertThat(receivedEvent.orderId()).isEqualTo(orders.get(0).getId());
                    assertThat(receivedEvent.orderStatus()).isEqualTo("PROCESSED");
                });
    }

private OrderPlacedEvent consumeKafkaEvent(String topic) {
    	
        // Kafka Consumer Configuration
        Properties props = getTestKafkaConfig(consumerFactory.getConfigurationProperties());

        try (KafkaConsumer<String, OrderPlacedEvent> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));

            ConsumerRecords<String, OrderPlacedEvent> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, OrderPlacedEvent> record : records) {
                return record.value();
            }
        }
        return null;
    }
    
    private Properties getTestKafkaConfig(Map<String, Object> consumerFactoryPropertiesMap) 
    {
    	 // Get Spring Boot's Kafka ConsumerFactory properties
         Properties props = new Properties();
         props.putAll(consumerFactoryPropertiesMap); // Get the exact properties
         props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID()); // Ensure a fresh consumer group
         logger.info("ConsumerFactory Props: {}", consumerFactoryPropertiesMap);
         logger.info("Final Consumer Properties: {}", props);
        return props;
    }
}