FROM ghcr.io/graalvm/graalvm-ce:ol8-java17-22.3.2 AS builder
RUN microdnf install -y gcc glibc-devel zlib-devel libstdc++-devel maven
RUN gu install native-image
LABEL maintainer="Exoquic <support@exoquic.com>"
LABEL description="Exoquic PostgreSQL Agent for capturing and streaming database changes"
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -Pnative -DskipTests
FROM oraclelinux:8-slim
WORKDIR /app
COPY --from=builder /app/target/exoquic-agent /app/exoquic-agent
RUN chmod +x /app/exoquic-agent
ENTRYPOINT ["/app/exoquic-agent"]
