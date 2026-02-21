# SpireDB Makefile

.PHONY: setup build test test-unit test-raft clean proto deps format docker-build build-rust test-rust clean-rust \
       agent code-search doc-qa knowledge-base offline-search

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

build-rust:
	cd compute && cargo build --release

test-rust:
	cd compute && cargo test

clean-rust:
	cd compute && cargo clean

clippy-rust:
	cd compute && cargo clippy --all-targets --all-features -- -D warnings

clippy-fix-rust:
	cd compute && __CARGO_FIX_YOLO=1 cargo clippy --fix --allow-dirty --allow-staged --all-targets --all-features

fmt-rust:
	cd compute && cargo fmt --all

check-fmt-rust:
	cd compute && cargo fmt --all -- --check

fullchecks: build format lint clippy-fix-rust clippy-rust fmt-rust test-rust

agent:
	./tools/run-agent.sh $(ARGS)

# make agent ARGS="-p /path/to/project"

code-search:
	./tools/run-example.sh code-search -- $(ARGS)

# make code-search ARGS="index ./src"
# make code-search ARGS="search 'error handling'"
# make code-search ARGS="symbol MyStruct"
# make code-search ARGS="interactive"

doc-qa:
	./tools/run-example.sh doc-qa -- $(ARGS)

# make doc-qa ARGS="ingest ./docs"
# make doc-qa ARGS="ask 'how does authentication work?'"
# make doc-qa ARGS="interactive"

knowledge-base:
	./tools/run-example.sh knowledge-base -- $(ARGS)

# make knowledge-base ARGS="seed"
# make knowledge-base ARGS="search 'billing'"
# make knowledge-base ARGS="watch"
# make knowledge-base ARGS="interactive"

offline-search:
	./tools/run-example.sh offline-search -- $(ARGS)

# make offline-search ARGS="index ./docs"
# make offline-search ARGS="search 'database migration'"
# make offline-search ARGS="interactive"

pf:
	kubectl port-forward svc/spiredb 6379:6379