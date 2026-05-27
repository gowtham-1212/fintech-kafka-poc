import { Kafka, Producer, Admin, logLevel } from "kafkajs";
import 'dotenv/config'; 

const brokers = process.env.KAFKA_BROKERS
  ? process.env.KAFKA_BROKERS.split(",")
  : ["localhost:9092", "localhost:9095", "localhost:9096"];

console.log(brokers, "brokers======")

export const kafka = new Kafka({
  clientId: "payment-gateway",
  brokers: brokers,
  logLevel: logLevel.WARN,
  ssl: false,
  sasl: {
    mechanism: "plain",
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD,
  },
});

let producer: Producer | null = null;
let admin: Admin | null = null;

export async function ensureTopics(topics: string[], numPartitions = 3) {
  const admin = kafka.admin();
  
  try {
    await admin.connect();
    console.info('Kafka SASL PLAIN authentication successful.');
    const existingTopics = await admin.listTopics();

    // 1. Create missing topics
    const topicsToCreate = topics.filter(
      (topic) => !existingTopics.includes(topic),
    );
    if (topicsToCreate.length > 0) {
      console.log(
        `Creating topics: ${topicsToCreate.join(", ")} with ${numPartitions} partitions`,
      );
      await admin.createTopics({
        topics: topicsToCreate.map((topic) => ({
          topic,
          numPartitions,
          replicationFactor: 3,
        })),
      });
    }

    // 2. Check and update partition counts for existing topics
    const existingTargetTopics = topics.filter((topic) =>
      existingTopics.includes(topic),
    );
    if (existingTargetTopics.length > 0) {
      const metadata = await admin.fetchTopicMetadata({
        topics: existingTargetTopics,
      });

      for (const topicMetadata of metadata.topics) {
        if (topicMetadata.partitions.length < numPartitions) {
          console.log(
            `Increasing partitions for ${topicMetadata.name} from ${topicMetadata.partitions.length} to ${numPartitions}`,
          );
          await admin.createPartitions({
            topicPartitions: [
              {
                topic: topicMetadata.name,
                count: numPartitions,
              },
            ],
          });
        }
      }
    }
  } catch (err) {
    console.error("Error in ensureTopics:", err);
  } finally {
    await admin.disconnect();
  }
}

export async function getProducer() {
  if (producer) return producer;

  producer = kafka.producer({
    idempotent: true,
    retry: {
      initialRetryTime: 100,
      retries: 10,
    },
  });
  await producer.connect();
  console.log("Kafka Producer connected");
  return producer;
}

export async function disconnectProducer() {
  if (producer) {
    await producer.disconnect();
    console.log("Kafka Producer disconnected");
  }
}
