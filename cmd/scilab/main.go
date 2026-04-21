package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"goadvanced/internal/science"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second) // Context timeout for graceful cancellation.
	defer cancel()

	model := science.LinearModel{ModelConfig: science.ModelConfig{Intercept: 1.25, Weights: []float64{0.5, 1.1, -0.2}}}
	store := science.NewJSONFileStore("results.jsonl")
	pipeline := science.NewPipeline(model, store, science.WithWorkers(3), science.WithPanicRecovery(true))

	data := []science.Vector[float64]{
		{Label: "exp-001", Values: []float64{12.5, 0.3, 4.2}, Time: time.Now()},
		{Label: "exp-002", Values: []float64{7.1, 3.4, 1.8}, Time: time.Now()},
		{Label: "exp-003", Values: []float64{2.0, 6.2, 0.5}, Time: time.Now()},
	}

	outs, err := pipeline.ProcessAll(ctx, data)
	if err != nil {
		var regErr science.RegressionError
		if errors.As(err, &regErr) {
			log.Fatalf("regression failure: %v", regErr)
		}
		log.Fatalf("pipeline failure: %v", err)
	}

	for _, out := range outs {
		fmt.Printf("%s => model=%s prediction=%.4f duration=%s\n", out.InputLabel, out.ModelName, out.Prediction, out.Duration)
	}
	fmt.Printf("total processed: %d\n", pipeline.ProcessedCount())
}
