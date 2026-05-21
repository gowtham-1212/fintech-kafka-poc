import { Kafka, Consumer, Admin, logLevel} from 'kafkajs';

const brokers = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:9092'];

export const kafka = new Kafka({
  clientId: 'notification-worker',
  brokers: brokers,
  logLevel: logLevel.ERROR
});

let consumer: Consumer | null = null;

export async function ensureTopics(topics: string[]) {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const existingTopics = await admin.listTopics();
    const topicsToCreate = topics.filter(topic => !existingTopics.includes(topic));
    if (topicsToCreate.length > 0) {
      console.log(`Creating topics: ${topicsToCreate.join(', ')}`);
      await admin.createTopics({
        topics: topicsToCreate.map(topic => ({
          topic,
          numPartitions: 1,
          replicationFactor: 1,
        })),
      });
    }
  } finally {
    await admin.disconnect();
  }
}

export async function getConsumer() {
  if (consumer) return consumer;
  consumer = kafka.consumer({ groupId: 'notification-worker-group' });
  await consumer.connect();
  console.log('Kafka Consumer connected');
  return consumer;
}

export async function disconnectKafka() {
  if (consumer) await consumer.disconnect();
  console.log('Kafka disconnected');
}
