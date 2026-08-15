# Lock Admin console — no-framework SPA behind OpenResty with a real 3-node
# lock cluster (pinned Lunet runtime) as the backend. Lifecycle logic lives
# in tooling/lib/consolectl.tl (typed, testable); the bin/*.sh wrappers are
# thin POSIX sh and carry no logic.

LUAROCKS ?= luarocks --lua-version=5.1 --tree=../.rocks
PORT     ?= 8480
WORKLOAD ?= on

LUALIB_LUA := $(wildcard openresty/lualib/*.lua)
TEST_LUA   := $(wildcard tests/*.lua)

.PHONY: init up down status logs types check clean

init:
	@ls ../.rocks/share/lua/5.1/tl.lua >/dev/null 2>&1 || { \
		echo "Installing Teal toolchain into ../.rocks ..."; \
		$(MAKE) -C .. deps; }
	@ls ../.rocks/share/lua/5.1/tl.lua >/dev/null 2>&1 || { \
		echo "ERROR: tl (Teal) not available in ../.rocks" >&2; exit 1; }
	@echo "console toolchain: OK"

up: init
	@$(MAKE) -C .. build lunet-runtime
	@PORT=$(PORT) WORKLOAD=$(WORKLOAD) bin/up.sh

down:
	@bin/down.sh

status:
	@bin/status.sh

logs:
	@tail -n 40 -f tmp/n1.log tmp/n2.log tmp/n3.log tmp/nginx-error.log 2>/dev/null

# Type-check the SPA from its JSDoc. No package.json, no node_modules, no
# build step: tsc only reads (noEmit) and the .mjs files ship as-is. Fast
# enough to run on its own as an inner loop.
types:
	tsc -p tsconfig.json

check: init
	tsc -p tsconfig.json
	shellcheck bin/*.sh
	@LUA_PATH="../.rocks/share/lua/5.1/?.lua;../.rocks/share/lua/5.1/?/init.lua;;" \
		../.rocks/bin/tl check tooling/lib/consolectl.tl
	@for f in $(LUALIB_LUA) $(TEST_LUA); do \
		luajit -bl "$$f" >/dev/null || { echo "luajit syntax-compile FAILED: $$f" >&2; exit 1; }; \
	done
	@echo "luajit syntax-compile: OK ($(LUALIB_LUA) $(TEST_LUA))"
	@luajit tests/lock_log_test.lua
	@luajit tests/lock_state_test.lua
	@luajit tests/break_client_test.lua
	@luajit tests/workload_test.lua

clean: down
	rm -rf tmp
