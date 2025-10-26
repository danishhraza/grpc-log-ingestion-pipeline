Log Analytics System

A simple distributed log analytics setup using gRPC, RabbitMQ, Elasticsearch, and Kibana.

1. Overview

This project simulates logs from multiple microservices, collects them through a gRPC collector, and indexes them into Elasticsearch for visualization in Kibana.

Architecture
[microservice-generator] → (gRPC) → [collector] → (RabbitMQ) → [worker] → (bulk) → [Elasticsearch] → [Kibana]


microservice-generator: generates synthetic logs from random services and sends them via gRPC.

collector: receives logs, validates them, and publishes them to RabbitMQ.

worker: consumes logs from RabbitMQ and bulk indexes them into Elasticsearch.

Elasticsearch + Kibana: store, search, and visualize logs.

⚙️ Setup
Prerequisites

Node.js v18+

RabbitMQ running on amqp://localhost:5672

Elasticsearch & Kibana running on http://localhost:9200 and http://localhost:5601

Environment

Each service uses environment variables:

RABBIT_URL=amqp://localhost:5672
ES_URL=http://localhost:9200

Run Services

In three terminals:

# 1. Start the collector
cd services/collector
npm start

# 2. Start the worker
cd services/worker
npm start

# 3. Start the microservice log generator
cd services/microservice-generator
npm start


Logs will appear in Kibana under the logs-* index pattern.

🪵 Log Format

Each log document includes:

{
  "timestamp": "2025-10-26T15:34:07.613Z",
  "service": "auth-service",
  "level": "INFO",
  "message": "synthetic log message",
  "latencyMs": 123.4,
  "attrs": { "route": "/api/test" }
}

📊 Viewing in Kibana

Go to Stack Management → Index Patterns

Create a data view for logs-*

Open Discover to explore incoming logs