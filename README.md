# openworks-pipeworx

Pipeline engine library for the OpenWorks ecosystem.

Provides job queue, worker polling, pipe matrix, and dynamic pipeline registration.
Used as a Go library — embedded by consumers, not run standalone.

## Consumers

- **opencloud** — embedded in `services/jobengine/`, MemoryStore backend
- **openworks-xis** — embedded with OracleStore backend + Graph API fassade

## Package Structure

```
pkg/
├── engine/       Job queue, poll endpoint, matrix, worker registry
├── config/       Pipeline YAML loader, config structs
└── (store.go)    JobStore interface (TODO: extract from engine)
```

## Part of the Kosmos Initiative

Built for [OpenCloud](https://codeberg.org/kosmos-eu) — sovereign, self-hosted cloud infrastructure.
