# Steps to run

1. Run a local temporal service or signup for a cloud account

Read about connecting to the account at https://docs.temporal.io/develop/go/temporal-clients

We use a very similar setup for connecting to temporal in this repo. Most of you would already have this connection implemented in your application.

We need to set envs to connect to your temporal namespace in the cloud account. You can leave these envs as unset if you are using a local temporal setup.

Default hostPort is `localhost:7233` and namespace is `default`

```
TEMPORAL_HOST_PORT='<namespace>.<account_id>.tmprl.cloud:7233'
TEMPORAL_NAMESPACE='<namespace>.<account_id>'

CERT_PATH='/path/to/ca.pem'
KEY_PATH='/path/to/ca.key'
```

2. Run the following command to start the worker
```
OTEL_SERVICE_NAME='temporal-worker' OTEL_EXPORTER_OTLP_ENDPOINT='ingest.<region>.signoz.cloud' OTEL_EXPORTER_OTLP_HEADERS="signoz-ingestion-key=<signoz-ingestion-key>" go run worker/main.go
```

3. Run the following command to start the example
```
OTEL_SERVICE_NAME='temporal-client' OTEL_EXPORTER_OTLP_ENDPOINT='ingest.<region>.signoz.cloud' OTEL_EXPORTER_OTLP_HEADERS="signoz-ingestion-key=<signoz-ingestion-key>" go run starter/main.go
```

You should start seeing sdk metrics and traces appearing in signoz. Logs (in the example) is not instrumented by otel and works natively like zerolog. You should write to file and read the logs file using otel-collector `filelogreceiver`

Code at `instrument/zerolog_apapter.go` wraps zerolog to implement Temporal's log.Logger interface and outputs to stdout. You can configure zerolog at `NewZerologAdapter()` method in the same file. This zerolog logger is used at worker and client code `logger := instrument.NewZerologAdapter()` which is used to pass as param to `client.Options` of `helloworld/connection.go` to set the logger framework to be used at temporal.

# Notes
If you want to send the metrics and traces to your local otelcollector, use the below run commands:
For Worker
```
OTEL_SERVICE_NAME='temporal-worker' INSECURE_MODE=true OTEL_EXPORTER_OTLP_ENDPOINT='http://localhost:4317' go run worker/main.go 
```
For Client
```
OTEL_SERVICE_NAME='temporal-client' INSECURE_MODE=true OTEL_EXPORTER_OTLP_ENDPOINT='http://localhost:4317' go run starter/main.go
```


