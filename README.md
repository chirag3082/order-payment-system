# Order & Payment Processing System

An event-driven microservices system that processes orders and payments with **crash-safe, at-least-once event delivery** between services. Built to demonstrate the distributed-systems patterns that keep money-moving workflows correct under failure.

> **Status: in active development.** Architecture and design are finalized (below); services are being implemented and pushed incrementally. This README is the source of truth for the design.

## Why this project

Order/payment flows have a classic hard problem: when a service commits to its database *and* must publish an event (e.g. "order placed" → charge payment), a crash between the two leaves the system inconsistent (the **dual-write problem**). This project solves that properly rather than hand-waving it.

## Architecture

```
   Client ──REST──▶  Order Service ──┐
                        │  (Postgres) │ Outbox table
                        └────────────▶│──poller──▶ Kafka ──▶ Payment Service ──▶ Postgres
                                                     │                │
                                                     │                └──▶ payment.result ──▶ Order Service (confirm)
                                                     └──▶ Dead-Letter Topic (after retries)
```

- **Order Service** — accepts orders over JWT-secured REST APIs, persists them, and writes events to a **transactional outbox** in the same DB transaction (no dual write).
- **Outbox poller** — relays committed outbox rows to Kafka, guaranteeing at-least-once delivery.
- **Payment Service** — consumes order events **idempotently** (dedupe by event id), simulates payment, emits `payment.result`.
- **Choreography saga** — `order → payment → confirmation` with no central orchestrator; each service reacts to events.
- **Reliability** — retry with backoff, **dead-letter topics** for poison messages, correlation-ID tracing across services.

## Patterns demonstrated

| Concern | Approach |
|---|---|
| Dual-write consistency | Transactional Outbox |
| Duplicate delivery | Idempotent consumers (event-id dedupe) |
| Distributed workflow | Choreography saga |
| Poison messages | Retry + dead-letter topic |
| Security | JWT/OAuth on REST APIs, input validation |
| Observability | Correlation-ID tracing, Actuator + Micrometer metrics |
| Correctness proof | Testcontainers integration tests, including crash-recovery scenarios |

## Tech stack

Java 21 · Spring Boot · Apache Kafka · PostgreSQL · Redis · Docker Compose · Testcontainers · JUnit 5

## Roadmap

- [ ] Order Service: entity model, REST API, outbox write
- [ ] Outbox poller → Kafka
- [ ] Payment Service: idempotent consumer + result event
- [ ] Saga completion + confirmation
- [ ] Dead-letter handling + retry
- [ ] JWT auth + input validation
- [ ] Testcontainers integration tests (incl. crash-recovery)
- [ ] Docker Compose one-command local run
- [ ] GitHub Actions CI
- [ ] Load test + results writeup

## Running locally

_Docker Compose setup lands with the first services — instructions will appear here._
