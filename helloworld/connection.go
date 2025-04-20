package helloworld

import (
	"context"
	"crypto/tls"
	"os"

	"github.com/SigNoz/temporal-opentelemetry-instrumentation/instrument"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
)

func NewClient(ctx context.Context, tracingInterceptor interceptor.ClientInterceptor, metricsHandler client.MetricsHandler, logger *instrument.ZerologAdapter) (client.Client, error) {
	// Get the key and cert from your env or local machine
	clientKeyPath := "/Users/ankitnayan/Desktop/temporal-certs/temporal-ca.key"
	clientCertPath := "/Users/ankitnayan/Desktop/temporal-certs/temporal-ca.pem"

	// Specify the host and port of your Temporal Cloud Namespace
	// Host and port format: namespace.unique_id.tmprl.cloud:port
	hostPort := "localhost:7233"
	namespace := "default"
	if os.Getenv("TEMPORAL_NAMESPACE") != "" {
		namespace = os.Getenv("TEMPORAL_NAMESPACE")
	}
	if os.Getenv("TEMPORAL_HOST_PORT") != "" {
		hostPort = os.Getenv("TEMPORAL_HOST_PORT")
	}

	// Use the crypto/tls package to create a cert object
	cert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		logger.Error("Unable to load cert and key pair", "error", err)
		return nil, err
	}

	// Add the cert to the tls certificates in the ConnectionOptions of the Client
	temporalClient, err := client.Dial(client.Options{
		HostPort:  hostPort,
		Namespace: namespace,
		ConnectionOptions: client.ConnectionOptions{
			TLS: &tls.Config{Certificates: []tls.Certificate{cert}},
		},
		Interceptors:   []interceptor.ClientInterceptor{tracingInterceptor},
		MetricsHandler: metricsHandler,
		Logger:         logger,
	})
	if err != nil {
		logger.Error("Unable to connect to Temporal Cloud", "error", err)
		return nil, err
	}

	logger.Info("Successfully connected to Temporal Cloud",
		"namespace", namespace,
		"hostPort", hostPort)
	return temporalClient, nil
}

func NewWorkerClient(ctx context.Context, tracingInterceptor interceptor.ClientInterceptor, metricsHandler client.MetricsHandler, logger *instrument.ZerologAdapter) (client.Client, error) {
	// Get the key and cert from your env or local machine
	clientKeyPath := "/Users/ankitnayan/Desktop/temporal-certs/temporal-ca.key"
	clientCertPath := "/Users/ankitnayan/Desktop/temporal-certs/temporal-ca.pem"

	// Specify the host and port of your Temporal Cloud Namespace
	// Host and port format: namespace.unique_id.tmprl.cloud:port
	hostPort := "integration.sdr7x.tmprl.cloud:7233"
	namespace := "integration.sdr7x"

	// Use the crypto/tls package to create a cert object
	cert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		logger.Error("Unable to load cert and key pair", "error", err)
		return nil, err
	}

	// Add the cert to the tls certificates in the ConnectionOptions of the Client
	temporalClient, err := client.Dial(client.Options{
		HostPort:  hostPort,
		Namespace: namespace,
		ConnectionOptions: client.ConnectionOptions{
			TLS: &tls.Config{Certificates: []tls.Certificate{cert}},
		},
		// Interceptors:   []interceptor.ClientInterceptor{tracingInterceptor},
		MetricsHandler: metricsHandler,
		Logger:         logger,
	})
	if err != nil {
		logger.Error("Unable to connect to Temporal Cloud", "error", err)
		return nil, err
	}

	logger.Info("Successfully connected to Temporal Cloud",
		"namespace", namespace,
		"hostPort", hostPort)
	return temporalClient, nil
}
