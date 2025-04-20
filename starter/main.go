package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/client"

	"github.com/rs/zerolog/log"
	"github.com/temporalio/samples-go/helloworld"
	"github.com/temporalio/samples-go/helloworld/instrument"
)

func main() {
	logger := instrument.NewZerologAdapter()

	ctx := context.Background()
	tp, mp, err := instrument.InitializeGlobalTelemetryProvider(ctx)
	if err != nil {
		log.Fatal().Msg(fmt.Sprintf("Unable to create a global trace provider: %v", err.Error()))
	}

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	tracingInterceptor, err := instrument.NewTracingInterceptor(instrument.TracerOptions{})
	if err != nil {
		log.Fatal().Msg(fmt.Sprintf("Unable to create interceptor: %v", err.Error()))
	}

	metricsHandler := instrument.NewOpenTelemetryMetricsHandler()

	// The client is a heavyweight object that should be created once per process.
	c, err := helloworld.NewClient(ctx, tracingInterceptor, metricsHandler, logger)
	if err != nil {
		log.Fatal().Msg(fmt.Sprintf("Unable to create client: %v", err.Error()))
	}
	defer c.Close()

	workflowID := fmt.Sprintf("hello_world_%s", uuid.New().String())
	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: "hello-world",
	}

	logger.Info("Starting workflow", "workflowID", workflowID)
	we, err := c.ExecuteWorkflow(ctx, workflowOptions, helloworld.Workflow, "Workflow Name 2")
	if err != nil {
		log.Fatal().Msg(fmt.Sprintf("Unable to execute workflow: %v", err.Error()))
	}

	// Synchronously wait for the workflow completion.
	var result string
	err = we.Get(ctx, &result)
	if err != nil {
		log.Fatal().Msg(fmt.Sprintf("Unable to get workflow result: %v", err.Error()))
	}
	log.Info().Msg(fmt.Sprintf("Workflow result: %v", result))

	// Create a context with timeout for graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Shutdown telemetry providers gracefully
	if err := tp.Shutdown(shutdownCtx); err != nil {
		log.Error().Msg(fmt.Sprintf("Error shutting down trace provider: %v", err.Error()))
	}
	if err := mp.Shutdown(shutdownCtx); err != nil {
		log.Error().Msg(fmt.Sprintf("Error shutting down meter provider: %v", err.Error()))
	}
}
