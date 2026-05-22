import { Kafka, Producer, Consumer, Admin, logLevel } from 'kafkajs';

const brokers = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:9092'];

export const kafka = new Kafka({
  clientId: 'analytics-service',
  brokers: brokers,
  logLevel: logLevel.WARN,
  ssl: false,
  sasl: {
    mechanism: "plain",
    username: "luffy",
    password: "kingofthepirate"
  }
});

let producer: Producer | null = null;
let consumer: Consumer | null = null;

export async function ensureTopics(topics: string[], numPartitions = 3) {
  const admin = kafka.admin();
  try {
    await admin.connect();
    console.info('Kafka SASL PLAIN authentication successful.');
    const existingTopics = await admin.listTopics();
    const topicsToCreate = topics.filter(topic => !existingTopics.includes(topic));
    if (topicsToCreate.length > 0) {
      await admin.createTopics({
        topics: topicsToCreate.map(topic => ({
          topic,
          numPartitions,
          replicationFactor: 1,
        })),
      });
    }
  } finally {
    await admin.disconnect();
  }
}

export async function getProducer() {
  if (producer) return producer;
  producer = kafka.producer();
  await producer.connect();
  return producer;
}

export async function getConsumer() {
  if (consumer) return consumer;
  consumer = kafka.consumer({ groupId: 'analytics-service-group' });
  await consumer.connect();
  return consumer;
}

export async function disconnectKafka() {
  if (producer) await producer.disconnect();
  if (consumer) await consumer.disconnect();
}
