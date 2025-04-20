package helloworld

import (
	"context"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

// Workflow is a Hello World workflow definition.
func Workflow(ctx workflow.Context, name string) (string, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	logger := workflow.GetLogger(ctx)
	logger.Info("HelloWorld workflow started", "name", name)

	var result1 string
	err := workflow.ExecuteActivity(ctx, Activity1, "Activity").Get(ctx, &result1)
	if err != nil {
		logger.Error("Activity failed", "error", err)
		return "", err
	}

	// Random sleep between 1-3 seconds
	sleepDuration := time.Second * time.Duration(1+workflow.Now(ctx).UnixNano()%3)
	logger.Info("Sleeping before Activity2", "duration", sleepDuration.String())
	workflow.Sleep(ctx, sleepDuration)

	var result2 string
	err = workflow.ExecuteActivity(ctx, Activity2, "Activity2").Get(ctx, &result2)
	if err != nil {
		logger.Error("Activity2 failed", "error", err)
		return "", err
	}

	finalResult := result1 + " " + result2

	// Use workflow start time for deterministic randomization
	randomValue := workflow.GetInfo(ctx).WorkflowStartTime.UnixNano() % 2
	if randomValue == 0 { // 50% chance
		logger.Info("Executing Activity3")
		var result3 string
		err = workflow.ExecuteActivity(ctx, Activity3, "Activity3").Get(ctx, &result3)
		if err != nil {
			logger.Error("Activity3 failed", "error", err)
			return "", err
		}
		finalResult += " " + result3
	} else {
		logger.Info("Skipping Activity3")
	}

	logger.Info("Workflow completed", "result", finalResult)
	return finalResult, nil
}

func Activity1(ctx context.Context, name string) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Activity started", "name", name)

	url := "https://signoz.io"
	client := http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)

	logger.Info("Sending request to Signoz", "url", url)
	res, err := client.Do(req)
	if err != nil {
		logger.Error("Failed to send request", "error", err)
		return "", err
	}

	body, err := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if err != nil {
		logger.Error("Failed to read response body", "error", err)
		return "", err
	}

	logger.Info("Received response from Signoz", "bytes", len(body))
	return "Hello " + name + "!", nil
}

func Activity2(ctx context.Context, name string) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Activity2 started", "name", name)

	// Simulate some work
	time.Sleep(500 * time.Millisecond)

	result := "Welcome back " + name + "!"
	logger.Info("Activity2 completed", "result", result)
	return result, nil
}

func Activity3(ctx context.Context, name string) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Activity3 started", "name", name)

	// Simulate different work
	time.Sleep(750 * time.Millisecond)

	result := "See you soon " + name + "!"
	logger.Info("Activity3 completed", "result", result)
	return result, nil
}
