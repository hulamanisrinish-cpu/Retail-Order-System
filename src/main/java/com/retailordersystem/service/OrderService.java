package com.retailordersystem.service;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.retailordersystem.event.OrderPlacedEvent;
import com.retailordersystem.model.Order;
import com.retailordersystem.repository.OrderRepository;

@Service
public class OrderService {
	
	private static final Logger logger = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, OrderPlacedEvent> kafkaTemplate; 

    
    public OrderService(OrderRepository orderRepository, KafkaTemplate<String, OrderPlacedEvent> kafkaTemplate) {
        this.orderRepository = orderRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

	public Order createOrder(Order order) {
    	Order savedOrder = orderRepository.save(order);
    	logger.info("Order {} saved to postgres:", savedOrder);
    	OrderPlacedEvent event = new OrderPlacedEvent(savedOrder.getId(),"PROCESSED","Event from OrderService ");
    	
        kafkaTemplate.send("order_placed_topic", event); // Publish event
        logger.info("Order {} published to kafka :", event);
        return savedOrder;
    }

    public Order getOrder(Long id) {
        return orderRepository.findById(id).orElseThrow(() -> new RuntimeException("Order not found"));
    }

    public List<Order> getAllOrders() {
        return orderRepository.findAll();
    }

    public Order updateOrder(Long id, Order order) {
        Order existingOrder = getOrder(id);
        existingOrder.setStatus(order.getStatus());
        return orderRepository.save(existingOrder);
    }

    public void deleteOrder(Long id) {
        Order order = getOrder(id);
        orderRepository.delete(order);
    }
}
