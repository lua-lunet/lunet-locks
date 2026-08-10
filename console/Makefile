# Lock Admin console — no-framework SPA behind nginx with a bun mock backend.
# Lifecycle logic lives in tooling/lib/consolectl.tl (typed, testable); the
# bin/*.sh wrappers are thin POSIX sh and carry no logic.

LUAROCKS ?= luarocks --lua-version=5.1 --tree=../.rocks
PORT     ?= 8480
MOCK_PORT ?= 8481

.PHONY: init up down status logs check clean

init:
	@ls ../.rocks/share/lua/5.1/tl.lua >/dev/null 2>&1 || { \
		echo "Installing Teal toolchain into ../.rocks ..."; \
		$(MAKE) -C .. deps; }
	@ls ../.rocks/share/lua/5.1/tl.lua >/dev/null 2>&1 || { \
		echo "ERROR: tl (Teal) not available in ../.rocks" >&2; exit 1; }
	@echo "console toolchain: OK"

up: init
	@PORT=$(PORT) MOCK_PORT=$(MOCK_PORT) bin/up.sh

down:
	@bin/down.sh

status:
	@bin/status.sh

logs:
	@tail -n 40 -f tmp/mock.log tmp/nginx-error.log 2>/dev/null

check: init
	shellcheck bin/*.sh
	@LUA_PATH="../.rocks/share/lua/5.1/?.lua;../.rocks/share/lua/5.1/?/init.lua;;" \
		../.rocks/bin/tl check tooling/lib/consolectl.tl

clean: down
	rm -rf tmp
