package com.retailordersystem.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.retailordersystem.constants.DockerImageConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.retailordersystem.event.OrderPlacedEvent;
import com.retailordersystem.model.Order;
import com.retailordersystem.repository.OrderRepository;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
public class OrderControllerIntegrationTestWithTestcontainers {

	private static final Logger logger = LoggerFactory.getLogger(OrderControllerIntegrationTestWithTestcontainers.class);
    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private OrderRepository orderRepository;
    
    @Autowired
    private ConsumerFactory<String, OrderPlacedEvent> consumerFactory;

    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    
    @Container
    static PostgreSQLContainer<?> postgreSQLContainer = 
        new PostgreSQLContainer<>(DockerImageName.parse(DockerImageConstants.POSTGRES_IMAGE));

    @Container
    static KafkaContainer kafkaContainer = 
        new KafkaContainer(DockerImageName.parse(DockerImageConstants.KAFKA_IMAGE));



    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgreSQLContainer::getJdbcUrl);
        registry.add("spring.datasource.username", postgreSQLContainer::getUsername);
        registry.add("spring.datasource.password", postgreSQLContainer::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
    }
    
    
    

    @Test
    void testCreateOrder() throws Exception {
    	//Clean up table first 
        orderRepository.deleteAll();
        // Given: Order request payload
        Order order = new Order();
        order.setStatus("NEW");

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
        assertThat(orders.get(0).getStatus()).isEqualTo("NEW");

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
    	
    	Map<String, Object> i = consumerFactory.getConfigurationProperties();
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
