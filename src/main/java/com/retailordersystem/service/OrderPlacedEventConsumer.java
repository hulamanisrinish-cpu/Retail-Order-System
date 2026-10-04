package com.retailordersystem.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//In KafkaConfig.java or a separate consumer class
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.retailordersystem.event.OrderPlacedEvent;
import com.retailordersystem.model.Order;
import com.retailordersystem.repository.OrderRepository;

@Service
public class OrderPlacedEventConsumer {
 private static final Logger logger = LoggerFactory.getLogger(OrderPlacedEventConsumer.class);

 private final OrderRepository orderRepository;

 public OrderPlacedEventConsumer(OrderRepository orderRepository) {
     this.orderRepository = orderRepository;
 }

 @KafkaListener(topics = "order_placed_topic")
 public void handleOrderPlacedEvent(OrderPlacedEvent  event) {
     try {
         Long orderId = event.orderId();
         Order order = orderRepository.findById(orderId)
                 .orElseThrow(() -> new RuntimeException("Order not found with ID: " + orderId));

         // --- Perform Order Processing Logic ---
         // This is where you'd simulate inventory checks, etc.
         // For now, let's just update the status to "PROCESSED"
         order.setStatus("PROCESSED"); 
         orderRepository.save(order);
         logger.info("Saved order: " + event.orderId()+" as "+order.getStatus()+",description :"+order.getDescription());

     } catch (NumberFormatException ex) {
        // System.err.println("Invalid order ID received: " + event.orderId());
         // Handle the error appropriately (e.g., log, send to a dead-letter queue)
     } catch (RuntimeException ex) {
         //System.err.println("Error processing order: " + ex.getMessage());
         // Handle the error (e.g., publish OrderFailedEvent)
     }
 }
}