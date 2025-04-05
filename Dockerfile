FROM maven:3.8.5-openjdk-17

LABEL maintainer="Exoquic <support@exoquic.com>"
LABEL description="Exoquic PostgreSQL Agent for capturing and streaming database changes"
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean install
ENV JAVA_OPTS="-Xms256m -Xmx512m"
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar target/exoquic-agent.jar"]
