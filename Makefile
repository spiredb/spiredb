# SpireDB Makefile

.PHONY: setup build test test-unit test-raft clean proto deps format docker-build

setup: deps
	@echo "Setup complete."

deps:
	cd spiredb && mix deps.get

build:
	cd spiredb && mix compile --warnings-as-errors

run:
	@echo "Running SpireDB..."
	-@pkill -9 -f 'beam.*spiredb' 2>/dev/null || true
	@rm -rf /tmp/spiredb_test* 2>/dev/null || true
	@rm -rf spiredb/test_data 2>/dev/null || true
	cd spiredb && iex --name spiredb@127.0.0.1 --cookie spiredb -S mix

test: test-unit
# test-raft disabled

test-unit:
	@echo "Running unit tests..."
	-@pkill -9 -f 'beam.*spiredb' 2>/dev/null || true
	@rm -rf /tmp/spiredb_test* 2>/dev/null || true
	@rm -rf spiredb/test_data 2>/dev/null || true
	@sleep 1
	cd spiredb && SPIRE_PD_START_RAFT=false mix test --exclude raft --exclude resp_integration

test-raft:
	@echo "Running Raft integration tests..."
	-@pkill -9 -f 'beam.*raft_test' 2>/dev/null || true
	@rm -rf /tmp/spiredb_raft_test* 2>/dev/null || true
	@rm -rf spiredb/test_data 2>/dev/null || true
	@sleep 1
	cd spiredb && elixir --name raft_test@127.0.0.1 --cookie spiredb_test -S mix test --only raft

clean:
	cd spiredb && mix clean
	rm -rf /tmp/spiredb* spiredb/test_data

proto:
	cd spiredb/apps/spiredb_common && \
	protoc --elixir_out=plugins=grpc:./lib/generated --elixir_opt=gen_descriptors=true \
	-I priv/proto priv/proto/*.proto

format:
	cd spiredb && mix format

check-format:
	cd spiredb && mix format --check-formatted

lint:
	cd spiredb && mix credo

docker-build:
	docker build -t spiredb:latest -f docker/Dockerfile spiredb/

pf:
	kubectl port-forward svc/spiredb 6379:6379