SHELL=/bin/bash

IMAGENAME="sigmod-18-contest:latest"
CONTAINERNAME="sigmod-18-container"

LOCALCODE="$(shell dirname $(shell pwd))"

.PHONY=build run clean

all: build

build: Dockerfile
	docker build \
		--build-arg localuser="$(shell whoami)" \
		--tag $(IMAGENAME) \
		.

run:
	docker run \
		--rm \
		--tty \
		--interactive \
		--memory 2g \
		--memory-swap 0 \
		--mount type=bind,source="$(LOCALCODE)",target="/code" \
		--cap-add=SYS_PTRACE \
		--security-opt seccomp=unconfined \
		--name $(CONTAINERNAME) \
		$(IMAGENAME)

clean:
	docker rmi $(IMAGENAME)
