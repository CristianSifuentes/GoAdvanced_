# Go Advanced Scientific Pipeline — Architecture Overview

This repository demonstrates an **expert-level Go project** with advanced language and engineering features used in production backends, scientific computing pipelines, and DevOps-grade services.

## Project Structure

```text
.
├── cmd/
│   └── scilab/
│       └── main.go
├── internal/
│   └── science/
│       └── pipeline.go
├── docs/
├── go.mod
├── PROJECT_STRUCTURE.md
├── README.md
└── LICENSE
```

## High-Level Design

- **`cmd/scilab/main.go`**
  - Application entrypoint.
  - Wires dependencies (model + storage + pipeline).
  - Demonstrates context timeouts, typed error handling (`errors.As`), and formatted reporting.

- **`internal/science/pipeline.go`**
  - Core domain and pipeline runtime.
  - Implements:
    - Generics (`Vector[T]`, `Float` constraints).
    - Interfaces + polymorphism (`ScientificModel`, `DataStore`).
    - Composition/embedding (`LinearModel` + `ModelConfig`).
    - Worker-pool concurrency (goroutines + channels + `select`).
    - Cancellation propagation (`context.Context`).
    - Panic recovery (`defer` + `recover`).
    - Synchronization (`Mutex`, `WaitGroup`, `Once`, `sync.Map`).
    - Lock-free metric tracking (`atomic.Int64`).
    - Custom errors (`RegressionError`) + wrapping/unwrapping.
    - Persistence abstraction through JSONL storage.

## Low-Level Feature Matrix

| Topic | Where Implemented | Why It Matters |
|---|---|---|
| Generics + constraints | `Vector[T Float]`, `type Float` | Reusable typed numeric models |
| Interfaces | `ScientificModel`, `DataStore` | Decoupled architecture, testability |
| Struct tags | JSON tags in `Vector`, `Result` | Serialization contracts |
| Composition | `LinearModel` embeds `ModelConfig` | Reuse without inheritance |
| Functional options | `Option`, `WithWorkers`, `WithPanicRecovery` | Extensible constructors |
| Worker pool | `ProcessAll` goroutines + channels | Throughput + bounded parallelism |
| Context cancellation | `select` on `ctx.Done()` | Graceful shutdown and timeouts |
| Panic safety | `recover` in workers | Fault containment |
| Error wrapping | `%w`, `Unwrap`, `errors.As` | Robust error diagnosis |
| Mutex + WaitGroup + Once | `JSONFileStore`, `ProcessAll` | Safe lifecycle and synchronization |
| Atomic counters | `processed atomic.Int64` | Lock-free metrics |
| Concurrent cache | `sync.Map` | Fast shared memoization |

## How to Run

```bash
go run ./cmd/scilab
```

## Expected Output (example)

```text
pipeline setup complete
exp-001 => model=linear_model prediction=7.5000 duration=...
exp-002 => model=linear_model prediction=6.0300 duration=...
exp-003 => model=linear_model prediction=8.1700 duration=...
total processed: 3
```

## Professional Notes

- The code is intentionally written with production patterns: explicit errors, deterministic shutdown, bounded concurrency, and clear dependency boundaries.
- Every advanced feature is explicitly marked with comments in code to make the learning objective obvious.
