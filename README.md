# Fintech Kafka POC: Event-Driven Payment System

This project is a Proof of Concept (POC) for a high-scale, event-driven fintech payment system using Kafka (KRaft), Fastify, and React.

## 🆕 New Features: Multi-Broker Kafka & Horizontal Scaling
The system has been upgraded to a **3-broker Kafka cluster** for high availability and **horizontal scaling**. 
- **Environment Variables**: Kafka credentials and brokers are now managed via `.env` files in each service.
- **Horizontal Scaling**: Demonstrate parallel processing by scaling the `transaction-engine`.

## ⚠️ Important: Remapped Ports
To avoid conflicts with existing services on your machine, all external ports have been remapped:

| Service | Host Port | Internal Port | URL |
| :--- | :--- | :--- | :--- |
| **Frontend Dashboard** | **5183** | 5173 | `http://localhost:5183` |
| **Kafka UI** | **8086** | 8080 | `http://localhost:8086` |
| **Redis Commander** | **8087** | 8081 | `http://localhost:8087` |
| **Payment Gateway** | **3011** | 3001 | `http://localhost:3011` |
| **Notification Worker** | **3013** | 3003 | `http://localhost:3013` |
| **Kafka Broker 1** | **9092** | 9092 | `localhost:9092` |
| **Kafka Broker 2** | **9095** | 9095 | `localhost:9095` |
| **Kafka Broker 3** | **9096** | 9096 | `localhost:9096` |
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

2.  **Scale the Transaction Engine** (Optional, for scaling test):
    ```bash
    docker compose up -d --scale transaction-engine=3
    ```

3.  **Run Scaling Test**:
    ```bash
    ./scripts/test-scaling.sh
    ```

4.  **Access the Dashboard**:
    Open `http://localhost:5183` in your browser.

5.  **Monitor Live Data**:
    - **Kafka UI**: `http://localhost:8086`
    - **Redis Commander**: `http://localhost:8087`
    - **MongoDB**: Connect to `mongodb://localhost:27018`

## Kafka Use Cases Demonstrated
- **Horizontal Scaling**: Use Kafka partitions to distribute load across multiple consumer instances. See [USE_CASE_SCALING.md](./USE_CASE_SCALING.md) for details.
- **Multi-Broker High Availability**: 3-broker cluster setup for resilience.
- **Persistence (Write-Behind)**: Transaction engine updates DB after processing.
- **Caching (Read-Aside)**: Analytics service uses Redis to keep aggregate state.
- **Fan-out**: Notification Worker consuming multiple topics for a single UI stream.
- **Dynamic Re-partitioning**: Code ensures topics have 3 partitions on startup.
