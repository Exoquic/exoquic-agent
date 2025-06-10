.PHONY: all help build build-docker run run-docker clean

NATIVE_IMAGE_NAME = exoquic-agent
DOCKER_IMAGE_NAME = exoquic/agent-native
DOCKER_TAG = latest

build:
	mvn -Pnative clean package -DskipTests

build-docker:
	docker build -t $(DOCKER_IMAGE_NAME):$(DOCKER_TAG) .

run:
	./target/$(NATIVE_IMAGE_NAME)

run-docker:
	docker run --name exoquic-agent-native -it --rm $(DOCKER_IMAGE_NAME):$(DOCKER_TAG)

clean:
	mvn clean
	rm -f target/$(NATIVE_IMAGE_NAME)
