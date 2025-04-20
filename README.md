### Steps to run this sample:
1) Run a Temporal Service (self-hosted or cloud account)
2) Run the following command to start the worker
```
OTEL_SERVICE_NAME='temporal-worker' OTEL_EXPORTER_OTLP_ENDPOINT='ingest.<region>.signoz.cloud' OTEL_EXPORTER_OTLP_HEADERS="signoz-ingestion-key=<signoz-ingestion-key>" go run worker/main.go
```
3) Run the following command to start the example
```
OTEL_SERVICE_NAME='temporal-client' OTEL_EXPORTER_OTLP_ENDPOINT='ingest.<region>.signoz.cloud' OTEL_EXPORTER_OTLP_HEADERS="signoz-ingestion-key=<signoz-ingestion-key>" go run starter/main.go
```
