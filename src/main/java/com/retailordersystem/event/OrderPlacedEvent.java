package com.retailordersystem.event;

public record OrderPlacedEvent (Long orderId,String orderStatus,String description) {}