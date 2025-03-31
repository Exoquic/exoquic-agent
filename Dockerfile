FROM eclipse-temurin:17-jre-alpine

LABEL maintainer="Exoquic <support@exoquic.com>"
LABEL description="Exoquic PostgreSQL Agent for capturing and streaming database changes"

# Create app directory
WORKDIR /app

# Create volume for offsets and logs
VOLUME /app/data
VOLUME /app/logs

# Copy JAR file and default config
COPY target/exoquic-agent.jar /app/
COPY src/main/resources/default-config.properties /app/config.properties

# Set environment variables
ENV JAVA_OPTS="-Xms256m -Xmx512m"

# Create directory for logs
RUN mkdir -p /app/logs

# Run the application
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar exoquic-agent.jar /app/config.properties"]
