# Fintech Kafka POC: Event-Driven Payment System

This project is a Proof of Concept (POC) for a high-scale, event-driven fintech payment system using Kafka (KRaft), Fastify, and React.

## 🆕 New Features: Data Persistence & Caching
The architecture now includes **MongoDB** for long-term persistence and **Redis** for real-time state caching.

## ⚠️ Important: Remapped Ports
To avoid conflicts with existing services on your machine (like local Kafka, Redis, or Mongo), all external ports have been remapped:

| Service | Host Port | Internal Port | URL |
| :--- | :--- | :--- | :--- |
| **Frontend Dashboard** | **5183** | 5173 | `http://localhost:5183` |
| **Kafka UI** | **8086** | 8080 | `http://localhost:8086` |
| **Redis Commander** | **8087** | 8081 | `http://localhost:8087` |
| **Payment Gateway** | **3011** | 3001 | `http://localhost:3011` |
| **Notification Worker** | **3013** | 3003 | `http://localhost:3013` |
| **Kafka Broker** | **9096** | 9092 | `localhost:9096` |
| **MongoDB** | **27018** | 27017 | `localhost:27018` |
| **Redis** | **6380** | 6379 | `localhost:6380` |

## Architecture

1.  **Payment Gateway**: REST API receiving payment requests and producing `payment.initiated`.
2.  **Transaction Engine**: 
    - Consumes `payment.initiated`.
    - **MongoDB**: Persists the transaction lifecycle (initiated -> completed/failed).
    - Produces `payment.completed` or `payment.failed`.
3.  **Analytics Service**: 
    - Consumes ALL payment events.
    - **Redis**: Caches the live metrics (Total Volume, Success Rate).
    - Produces `analytics.metrics`.
4.  **Notification Worker**: Consumes transactions and analytics to stream them to the UI via SSE.
5.  **Frontend Dashboard**: Side-bar navigation with Transactions and Analytics pages.

## Getting Started

### Prerequisites
- Docker and Docker Compose

### Running the System
1.  **Start the entire stack**:
    ```bash
    docker compose up --build
    ```

2.  **Access the Dashboard**:
    Open `http://localhost:5183` in your browser.

3.  **Monitor Live Data**:
    - **Kafka UI**: `http://localhost:8086`
    - **Redis Commander**: `http://localhost:8087`
    - **MongoDB**: Connect to `mongodb://localhost:27018`

## Kafka Use Cases Demonstrated
- **Persistence (Write-Behind)**: Transaction engine updates DB after processing.
- **Caching (Read-Aside)**: Analytics service uses Redis to keep aggregate state.
- **Fan-out**: Notification Worker consuming multiple topics for a single UI stream.
- **Dynamic Re-partitioning**: Code ensures topics have 3 partitions on startup.
