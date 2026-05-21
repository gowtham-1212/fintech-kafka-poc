# Fintech Kafka POC: Event-Driven Payment System

This project is a Proof of Concept (POC) for a high-scale, event-driven fintech payment system using Kafka (KRaft), Fastify, and React.

## Architecture

1.  **Payment Gateway**: REST API receiving payment requests and producing `payment.initiated` events.
2.  **Transaction Engine**: Consumes `payment.initiated`, simulates validation/processing, and produces `payment.completed` or `payment.failed`.
3.  **Notification Worker**: Consumes all payment events and streams them to the frontend via Server-Sent Events (SSE).
4.  **Frontend Dashboard**: React application showing a real-time feed of Kafka events.

## Tech Stack

- **Backend**: Node.js, Fastify, TypeScript, KafkaJS
- **Frontend**: React, Vite, TailwindCSS, TypeScript
- **Infrastructure**: Docker, Kafka (KRaft mode), MongoDB, Redis

## Getting Started

### Prerequisites

- Docker and Docker Compose

### Running the System

1.  **Start the infrastructure and services**:
    ```bash
    docker compose up --build
    ```

2.  **Access the Dashboard**:
    Open `http://localhost:5173` in your browser.

3.  **Trigger a Payment**:
    Use cURL to send a payment request to the Gateway:
    ```bash
    curl -X POST http://localhost:3001/api/pay \
      -H "Content-Type: application/json" \
      -d '{
        "userId": "user_123",
        "amount": 150.00,
        "receiverId": "merchant_999"
      }'
    ```

## Kafka Topic Flow

1.  `payment-gateway` -> `payment.initiated` -> Kafka
2.  Kafka -> `transaction-engine` (Consumes `payment.initiated`)
3.  `transaction-engine` -> `payment.completed` OR `payment.failed` -> Kafka
4.  Kafka -> `notification-worker` (Consumes `payment.*`)
5.  `notification-worker` -> SSE -> `frontend-dashboard`
# fintech-kafka-poc
