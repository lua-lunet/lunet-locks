# lunet-advisory-lock

`lunet-advisory-lock` is a runnable, fixed-membership advisory-lock service. It
orders lock requests with [vrr-core](https://github.com/lua-lunet/vrr-core),
serves newline-delimited JSON over TCP, and uses expiring leases rather than
mandatory locking.

Run a three-replica smoke test with the project-local Lunet runtime:

```console
make smoke
```

The command fetches the official Lunet `v0.7.2` release into
`.lunet/v0.7.2/`; it never uses a host `lunet-run` from `PATH`. See the
[documentation](docs/src/index.md) for configuration, the client protocol, and
operational limits.

Run the thirty-second three-datacenter lease-failover demonstration with:

```console
make simulation
```

It drives the service over TCP NDJSON, stores logs under `.tmp/`, and always
stops its three local node processes. Use `make simulation SIM_DURATION=10`
for a shorter development run (the maximum is 30 seconds).
