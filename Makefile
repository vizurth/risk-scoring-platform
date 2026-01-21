PROTO_DIR=internal/api/grpc
GEN_DIR=internal/api/gen

.PHONY: proto
proto:
	protoc \
		-I$(PROTO_DIR) \
		--go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/scoring.proto

. PHONY: up
up:
	docker-compose -f docker-compose.yaml up -d --build

.PHONY: down
down:
	docker-compose -f docker-compose.yaml down

install-deps:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest
	GOBIN=$(LOCAL_BIN) go install github.com/envoyproxy/protoc-gen-validate
	GOBIN=$(LOCAL_BIN) go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@v2.20.0

get-deps:
	go get -u google.golang.org/protobuf/cmd/protoc-gen-go
	go get -u google.golang.org/grpc/cmd/protoc-gen-go-grpc
	go get -u github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway
	go get -u github.com/envoyproxy/protoc-gen-validate

vendor-proto:
		@if [ ! -d third_party/google ]; then \
			git clone https://github.com/googleapis/googleapis third_party/googleapis &&\
			mkdir -p  third_party/google/ &&\
			mv third_party/googleapis/google/api third_party/google &&\
			rm -rf third_party/googleapis ;\
		fi
		@if [ ! -d third_party/protoc-gen-openapiv2 ]; then \
			mkdir -p third_party/protoc-gen-openapiv2/options &&\
			git clone https://github.com/grpc-ecosystem/grpc-gateway third_party/openapiv2 &&\
			mv third_party/openapiv2/protoc-gen-openapiv2/options/*.proto third_party/protoc-gen-openapiv2/options &&\
			rm -rf third_party/openapiv2 ;\
		fi
