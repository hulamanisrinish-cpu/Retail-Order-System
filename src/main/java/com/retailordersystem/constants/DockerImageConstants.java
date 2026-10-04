package com.retailordersystem.constants;

/**
 * Defines Docker image names used across the application.
 * This class cannot be instantiated or extended.
 */
public final class DockerImageConstants {

    // PostgreSQL Docker image
    public static final String POSTGRES_IMAGE = "postgres:16-alpine";

    // Kafka Docker image
    public static final String KAFKA_IMAGE = "confluentinc/cp-kafka:7.9.0";

    // Private constructor to prevent instantiation
    private DockerImageConstants() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
    }
}
