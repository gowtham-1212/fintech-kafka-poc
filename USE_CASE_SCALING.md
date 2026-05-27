# Use Case: Kafka Horizontal Scaling with Multiple Brokers

## Scenario: High-Volume Payment Processing

In a fintech application, the transaction engine is often the most resource-intensive component. It performs complex validations, fraud checks, and interacts with external banking APIs. During peak periods (e.g., Black Friday or salary days), the volume of "Payment Initiated" events can spike significantly.

### The Problem
A single instance of the `transaction-engine` might not be able to keep up with the incoming stream of events, leading to:
- Increased end-to-end latency for payments.
- Growing consumer lag in Kafka.
- Risk of service instability.

### The Solution: Horizontal Scaling with Multiple Kafka Brokers
By leveraging Kafka's architecture, we can scale the `transaction-engine` horizontally:

1.  **Multiple Brokers:** We've configured a 3-broker Kafka cluster (`kafka1`, `kafka2`, `kafka3`). This provides high availability and distributes the data load across multiple nodes.
2.  **Partitions:** The `payment.initiated` topic is configured with **3 partitions** (matching the number of brokers for optimal distribution).
3.  **Consumer Groups:** All instances of the `transaction-engine` belong to the `transaction-engine-group`.
4.  **Scaling:** When we spin up multiple instances of `transaction-engine`, Kafka automatically assigns each partition to exactly one consumer instance in the group.

### How to Test
1.  **Start the Cluster:**
    ```bash
    docker-compose up -d --build
    ```
2.  **Scale the Transaction Engine:**
    ```bash
    docker-compose up -d --scale transaction-engine=3
    ```
3.  **Run the Load Simulation:**
    ```bash
    ./scripts/test-scaling.sh
    ```
4.  **Verify Parallel Processing:**
    Check the logs of the transaction engines:
    ```bash
    docker-compose logs -f transaction-engine
    ```
    You will observe that different instances (e.g., `transaction-engine-1`, `transaction-engine-2`, `transaction-engine-3`) are processing different transaction IDs simultaneously, demonstrating horizontal scaling in action.

### Advantages
- **Throughput:** Processing capacity scales linearly with the number of instances (up to the partition count).
- **Fault Tolerance:** If one broker or one consumer instance fails, the others continue to operate, and Kafka rebalances the workload.
- **Data Locality:** By using multiple brokers, data is spread across the cluster, preventing any single node from becoming a bottleneck.
