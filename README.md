# lunet-locks

`lunet-locks` is a runnable, fixed-membership advisory-lock service. It
orders lock requests with [vrr-core](https://github.com/lua-lunet/vrr-core),
a sans-IO Viewstamped-Replication-Revisited core written in Rust, serves
newline-delimited JSON over TCP, and uses expiring leases rather than
mandatory locking. A small Rust adapter drives the core and owns the lock
state machine; the service process is LuaJIT and libuv (Lunet). Protocol
state lives in quorum memory: a restarted node recovers from the surviving
members, and the only bytes fsynced locally are the recovery nonce file.

Run a three-replica smoke test with the project-local Lunet runtime:

```console
make smoke
```

The command fetches the official Lunet `v0.8.0` release into
`.lunet/v0.8.0/`. See the
[documentation](docs/src/index.md) for configuration, the client protocol, and
operational limits.

Run the thirty-second three-datacenter lease-failover demonstration with:

```console
make simulation
```

It drives the service over TCP NDJSON, stores logs under `.tmp/`, and always
stops its three local node processes. Use `make simulation SIM_DURATION=10`
for a shorter development run (the maximum is 30 seconds).

On a local Colima Docker daemon, run the same dynamic-client simulation against
three stable containers with `make docker-simulation`. It uses a plain Docker
build and named Docker volumes—never BuildKit or bind mounts.
