package science

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Float is a type constraint demonstrating Go generics with type sets.
type Float interface {
	~float32 | ~float64
}

// Vector demonstrates a generic scientific data container.
type Vector[T Float] struct {
	Label  string    `json:"label"`  // Struct tag for JSON serialization.
	Values []T       `json:"values"` // Slice field for numeric observations.
	Time   time.Time `json:"time"`   // Standard-library time integration.
}

// ScientificModel defines behavior via interfaces (polymorphism).
type ScientificModel interface {
	Name() string
	Predict(v Vector[float64]) (float64, error)
}

// LinearModel demonstrates composition via embedded config.
type LinearModel struct {
	ModelConfig
}

// ModelConfig stores coefficients and implements reusable configuration.
type ModelConfig struct {
	Intercept float64
	Weights   []float64
}

// Name demonstrates a simple value-receiver method.
func (m LinearModel) Name() string { return "linear_model" }

// Predict demonstrates explicit error handling and numeric logic.
func (m LinearModel) Predict(v Vector[float64]) (float64, error) {
	if len(v.Values) != len(m.Weights) {
		return 0, fmt.Errorf("predict: dimensional mismatch: got=%d want=%d", len(v.Values), len(m.Weights))
	}
	sum := m.Intercept
	for i := range v.Values {
		sum += v.Values[i] * m.Weights[i]
	}
	if math.IsNaN(sum) || math.IsInf(sum, 0) {
		return 0, errors.New("predict: unstable result")
	}
	return sum, nil
}

// RegressionError is a custom error type with metadata.
type RegressionError struct {
	Stage string
	Err   error
}

// Error satisfies the built-in error interface.
func (e RegressionError) Error() string {
	return fmt.Sprintf("stage=%s err=%v", e.Stage, e.Err)
}

// Unwrap enables errors.Is/errors.As traversal.
func (e RegressionError) Unwrap() error { return e.Err }

// DataStore abstracts persistence for dependency inversion.
type DataStore interface {
	Save(ctx context.Context, out Result) error
}

// JSONFileStore is a concrete storage implementation.
type JSONFileStore struct {
	path string
	mu   sync.Mutex // Mutex guards concurrent writes.
}

// NewJSONFileStore constructs a writer-backed repository.
func NewJSONFileStore(path string) *JSONFileStore {
	return &JSONFileStore{path: path}
}

// Save writes result records to a JSONL file.
func (s *JSONFileStore) Save(ctx context.Context, out Result) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("save open: %w", err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	if err := enc.Encode(out); err != nil {
		return fmt.Errorf("save encode: %w", err)
	}
	return nil
}

// Result carries outputs and metadata for observability.
type Result struct {
	InputLabel string        `json:"input_label"`
	ModelName  string        `json:"model_name"`
	Prediction float64       `json:"prediction"`
	Duration   time.Duration `json:"duration"`
}

// Pipeline demonstrates advanced concurrency orchestration.
type Pipeline struct {
	model        ScientificModel
	store        DataStore
	workerCount  int
	recoverPanic bool
	processed    atomic.Int64 // Lock-free metric with atomics.
	once         sync.Once    // One-time setup semantics.
	cache        sync.Map     // Concurrent map for memoization.
}

// Option demonstrates the functional options pattern.
type Option func(*Pipeline)

// WithWorkers configures bounded worker-pool size.
func WithWorkers(n int) Option {
	return func(p *Pipeline) {
		if n > 0 {
			p.workerCount = n
		}
	}
}

// WithPanicRecovery toggles panic recovery in worker goroutines.
func WithPanicRecovery(enabled bool) Option {
	return func(p *Pipeline) {
		p.recoverPanic = enabled
	}
}

// NewPipeline constructs an immutable-style service with options.
func NewPipeline(model ScientificModel, store DataStore, opts ...Option) *Pipeline {
	p := &Pipeline{model: model, store: store, workerCount: 4, recoverPanic: true}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// ProcessAll runs vectors through a worker pool, uses channels + context + select.
func (p *Pipeline) ProcessAll(ctx context.Context, in []Vector[float64]) ([]Result, error) {
	p.once.Do(func() {
		fmt.Println("pipeline setup complete") // sync.Once ensures exactly-once setup.
	})

	jobs := make(chan Vector[float64])
	results := make(chan Result)
	errs := make(chan error, 1)

	var wg sync.WaitGroup
	for i := 0; i < p.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) { // Goroutine per worker.
			defer wg.Done()
			if p.recoverPanic {
				defer func() {
					if r := recover(); r != nil { // panic/recover pattern.
						select {
						case errs <- fmt.Errorf("worker %d panic: %v", workerID, r):
						default:
						}
					}
				}()
			}
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-jobs:
					if !ok {
						return
					}
					start := time.Now()
					if cached, ok := p.cache.Load(v.Label); ok {
						results <- cached.(Result)
						continue
					}
					pred, err := p.model.Predict(v)
					if err != nil {
						wrapped := RegressionError{Stage: "predict", Err: err}
						select {
						case errs <- wrapped:
						default:
						}
						continue
					}
					out := Result{InputLabel: v.Label, ModelName: p.model.Name(), Prediction: pred, Duration: time.Since(start)}
					p.processed.Add(1)
					p.cache.Store(v.Label, out)
					if err := p.store.Save(ctx, out); err != nil {
						select {
						case errs <- fmt.Errorf("persist: %w", err):
						default:
						}
						continue
					}
					results <- out
				}
			}
		}(i)
	}

	go func() {
		defer close(jobs)
		for _, sample := range in {
			select {
			case <-ctx.Done():
				return
			case jobs <- sample:
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	collected := make([]Result, 0, len(in))
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-errs:
			if err != nil {
				return nil, err
			}
		case out, ok := <-results:
			if !ok {
				return collected, nil
			}
			collected = append(collected, out)
		}
	}
}

// ProcessedCount exposes atomic metrics safely.
func (p *Pipeline) ProcessedCount() int64 {
	return p.processed.Load()
}
