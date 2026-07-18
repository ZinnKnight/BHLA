PROTO_DIR  := proto
GO_MODULE  := BHLA
PROTOC     ?= protoc

PROTO_FILES := \
	$(PROTO_DIR)/auth_service.proto \
	$(PROTO_DIR)/user_service.proto \
	$(PROTO_DIR)/order_service.proto \
	$(PROTO_DIR)/market_service.proto \
	$(PROTO_DIR)/saga_events.proto

.PHONY: tools proto tidy build clean

tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/envoyproxy/protoc-gen-validate@latest

proto:
	$(PROTOC) -I $(PROTO_DIR) \
		--go_out=. --go_opt=module=$(GO_MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(GO_MODULE) \
		--validate_out="lang=go,module=$(GO_MODULE):." \
		$(PROTO_FILES)

tidy:
	cd proto   && go mod tidy
	cd shared  && go mod tidy
	cd services/auth_service   && go mod tidy
	cd services/user_service   && go mod tidy
	cd services/order_service  && go mod tidy
	cd services/market_service && go mod tidy

build:
	go build ./...

clean:
	find proto -type d -mindepth 1 -maxdepth 1 ! -name proto_validation -exec rm -rf {} +
