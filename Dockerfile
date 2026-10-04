# ---------------------------------------------------------------------------
# Retail Order System — runtime image
#
# Build the jar first with:  mvn clean package
# then build the image with: docker build -t retail-order-system .
# ---------------------------------------------------------------------------
FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

# Copy the application jar produced by the Maven build
COPY target/retail-order-system-1.0.0.jar /app/app.jar

# Expose the HTTP port the API runs on
EXPOSE 8080

# Run the application
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
