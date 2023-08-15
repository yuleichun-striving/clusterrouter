REGISTRY_NAME=xxxx
GIT_COMMIT=$(shell git rev-parse "HEAD^{commit}")
VERSION=$(shell git describe --tags --abbrev=14 "${GIT_COMMIT}^{commit}" --always)
BUILD_TIME=$(shell TZ=Asia/Shanghai date +%FT%T%z)

CMDS=build-clusterrouter
all: build

build: clusterrouter

clusterrouter:
	mkdir -p bin
	CGO_ENABLED=0 GOOS=linux go build -ldflags "-X 'main.buildVersion=$(VERSION)' -X 'main.buildTime=${BUILD_TIME}'" -o ./bin/clusterrouter ./cmd/virtualnode-manager

container: container-clusterrouter

container-clusterrouter: clusterrouter
	docker build -t $(REGISTRY_NAME)/clusterrouter:$(VERSION) -f Dockerfile.clusterrouter --label revision=$(REV) .
