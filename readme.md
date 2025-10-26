Log Analytics System

A simple distributed log analytics setup using gRPC, RabbitMQ, Elasticsearch, and Kibana.

1. Overview

gRPC Service — Receives structured logs from client microservices.

RabbitMQ — Buffers and distributes log messages between the collector and workers.

Elasticsearch — Stores indexed logs for search and analysis.

Kibana — Provides a UI to view and filter logs.

2. Flow

Microservices send logs via gRPC (SendLog / SendLogs).

Collector pushes incoming logs to RabbitMQ.

Workers consume messages, format them, and bulk-index into Elasticsearch.

Logs appear in Kibana dashboards (filtered via data views).

3. Setup

# Start dependencies
docker-compose up -d

# Run collector
npm run start:collector

# Run worker
npm run start:worker


Make sure Elasticsearch and Kibana are running locally on default ports.

4. Example

A sample log document stored in Elasticsearch:

{
  "service": "payment-service",
  "level": "INFO",
  "message": "synthetic log from payment-service",
  "timestamp": "2025-10-26T15:34:07Z",
  "latencyMs": 257.9
}