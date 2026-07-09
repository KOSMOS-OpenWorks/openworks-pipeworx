# openworks-pipeworx

Pipeline engine library for the OpenWorks ecosystem.

Provides job queue, worker polling, pipe matrix, dynamic pipeline registration,
and an optional backend store protocol. Used as a Go library — embedded by
consumers, not run standalone.

## Features

- **Job Queue**: submit, cancel, expire, priority, dependencies, rate limiting
- **Worker Polling**: capacity-aware assignment, heartbeat, backpressure (429)
- **Pipe Matrix**: per-worker job type authorization with slot limits
- **Dynamic Pipelines**: workers register pipeline definitions on first poll
- **regToken**: registration persistence across engine restarts
- **StoreProvider** (optional): backend job store protocol with audit trail
- **Worker Tracking**: capacity/running per worker, online detection

## API Endpoints

Registered via `engine.RegisterRoutes(chiRouter)`:

### Core (`/api/v0/jobs/*`)
| Method | Path | Description |
|---|---|---|
| POST | `/` | Submit job |
| GET | `/all` | List all jobs (admin) |
| GET | `/{jobId}` | Get job status |
| DELETE | `/{jobId}` | Cancel job |
| GET | `/pipelines` | List registered pipelines |
| GET | `/workers` | List workers (capacity, running, online) |
| POST | `/workers/poll` | Worker poll endpoint |
| GET | `/stats` | Pipeline stats |
| GET | `/matrix` | Get pipe matrix |
| PUT | `/matrix` | Set pipe matrix (persists to YAML) |

### Store (`/api/v0/store/*`) — optional
| Method | Path | Description |
|---|---|---|
| GET | `/info` | Store type, DSN, status |
| GET | `/jobs` | List backend jobs (native state) |
| POST | `/jobs` | Inject job (simulate webapp) |
| GET | `/jobs/{jid}` | Get job + logs |
| GET | `/jobs/{jid}/audit` | Chronological audit trail |
| POST | `/jobs/{jid}/reset` | Reset to NEW |
| GET | `/services` | Service type definitions |
| GET | `/processors` | Heartbeat records |

Returns 404 when no StoreProvider is configured (OpenCloud).

## Interfaces

```go
// AuthExtractor — required, extracts user from HTTP request
type AuthExtractor interface {
    ExtractUser(r *http.Request) (*UserInfo, bool)
}

// StoreProvider — optional, backend job store
type StoreProvider interface {
    ListJobs(state *int, limit int) ([]StoreJob, error)
    GetJob(jid int64) (*StoreJob, []StoreLogEntry, error)
    InjectJob(serviceID int, parameter string, priority int) (int64, error)
    ResetJob(jid int64) error
    ListServices() ([]StoreService, error)
    ListProcessors() ([]StoreProcessor, error)
    Info() StoreInfo
    Audit(entry AuditEntry)
    GetAudit(jid int64) []AuditEntry
}
```

## Worker Capacity Tracking

The engine tracks each worker's reported capacity and running job count:

```go
engine.AvailableCapacity() // sum of (capacity - running) for online workers
engine.QueuedJobCount()    // unassigned queued jobs
```

Workers API returns per-worker stats:
```json
{"id": "worker-1", "capacity": 4, "running": 2, "online": true, "pick": ["pdfa-pdf"]}
```

## Consumers

- **OpenCloud** — `services/jobengine/`, RevaAuthExtractor, no StoreProvider
- **openworks-xis** — OracleStore/PostgresStore, SOAP+WebDAV facade, feeder
- **openworks-tui** — terminal UI, auto-detects Store tab

## Usage

```go
import (
    "codeberg.org/kosmos-openworks/openworks-pipeworx/pkg/engine"
    "codeberg.org/kosmos-openworks/openworks-pipeworx/pkg/config"
)

cfg := config.PipelineDefaults()
cfg.LoadPipelineDir("pipelines.d")

eng := engine.New(cfg, myAuthExtractor)
defer eng.Shutdown()

eng.LoadMatrix("matrix.yaml")
eng.SetStoreProvider(myStoreProvider) // optional

eng.RegisterRoutes(chiRouter)
```

## Part of the Kosmos Initiative

Built for [OpenCloud](https://codeberg.org/kosmos-eu) — sovereign, self-hosted cloud infrastructure.
