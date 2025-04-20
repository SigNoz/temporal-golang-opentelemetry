package main

import (
	"context"

	"go.temporal.io/sdk/worker"

	"github.com/SigNoz/temporal-opentelemetry-instrumentation/helloworld"
	"github.com/SigNoz/temporal-opentelemetry-instrumentation/instrument"
)

func main() {
	ctx := context.Background()

	// Create a new Zerolog adapter.
	logger := instrument.NewZerologAdapter()

	tp, mp, err := instrument.InitializeGlobalTelemetryProvider(ctx)
	if err != nil {
		logger.Error("Unable to create a global trace provider", "error", err)
	}

	defer func() {
		if err := tp.Shutdown(ctx); err != nil {
			logger.Error("Error shutting down trace provider", "error", err)
		}
		if err := mp.Shutdown(ctx); err != nil {
			logger.Error("Error shutting down meter provider", "error", err)
		}
	}()

	// Create interceptor
	tracingInterceptor, err := instrument.NewTracingInterceptor(instrument.TracerOptions{
		DisableSignalTracing: false,
		DisableQueryTracing:  false,
		DisableBaggage:       false,
	})
	if err != nil {
		logger.Error("Unable to create interceptor", "error", err)
	}

	// Create metrics handler
	metricsHandler := instrument.NewOpenTelemetryMetricsHandler()

	// The client is a heavyweight object that should be created once per process.
	c, err := helloworld.NewClient(ctx, tracingInterceptor, metricsHandler, logger)
	if err != nil {
		logger.Error("Unable to create client", "error", err)
		return
	}
	defer c.Close()

	// Create a new worker with the interceptor
	w := worker.New(c, "hello-world", worker.Options{
		// Interceptors: []interceptor.WorkerInterceptor{tracingInterceptor.(interceptor.WorkerInterceptor)},
	})

	w.RegisterWorkflow(helloworld.Workflow)
	w.RegisterActivity(helloworld.Activity1)
	w.RegisterActivity(helloworld.Activity2)
	w.RegisterActivity(helloworld.Activity3)

	// Start listening to the Task Queue.
	err = w.Run(worker.InterruptCh())
	if err != nil {
		logger.Error("Unable to start worker", "error", err)
		return
	}
}
