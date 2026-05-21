import { Kafka, Producer, Admin } from 'kafkajs';

const brokers = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:9092'];

export const kafka = new Kafka({
  clientId: 'payment-gateway',
  brokers: brokers,
});

let producer: Producer | null = null;
let admin: Admin | null = null;

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

export async function getProducer() {
  if (producer) return producer;
  
  producer = kafka.producer();
  await producer.connect();
  console.log('Kafka Producer connected');
  return producer;
}

export async function disconnectProducer() {
  if (producer) {
    await producer.disconnect();
    console.log('Kafka Producer disconnected');
  }
}
