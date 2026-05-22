import { Kafka, Consumer, Admin, logLevel} from 'kafkajs';

const brokers = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:9092'];

export const kafka = new Kafka({
  clientId: 'notification-worker',
  brokers: brokers,
  logLevel: logLevel.WARN,
  ssl: false,
  sasl: {
    mechanism: 'plain',
    username: "luffy",
    password: "kingofthepirate",
  },
});

let consumer: Consumer | null = null;

export async function ensureTopics(topics: string[], numPartitions = 3) {
  const admin = kafka.admin();
  
  try {
    await admin.connect();
    console.info('Kafka SASL PLAIN authentication successful.');
    const existingTopics = await admin.listTopics();
    
    // 1. Create missing topics
    const topicsToCreate = topics.filter(topic => !existingTopics.includes(topic));
    if (topicsToCreate.length > 0) {
      console.log(`Creating topics: ${topicsToCreate.join(', ')} with ${numPartitions} partitions`);
      await admin.createTopics({
        topics: topicsToCreate.map(topic => ({
          topic,
          numPartitions,
          replicationFactor: 1,
        })),
      });
    }

    // 2. Check and update partition counts for existing topics
    const existingTargetTopics = topics.filter(topic => existingTopics.includes(topic));
    if (existingTargetTopics.length > 0) {
      const metadata = await admin.fetchTopicMetadata({ topics: existingTargetTopics });
      
      for (const topicMetadata of metadata.topics) {
        if (topicMetadata.partitions.length < numPartitions) {
          console.log(`Increasing partitions for ${topicMetadata.name} from ${topicMetadata.partitions.length} to ${numPartitions}`);
          await admin.createPartitions({
            topicPartitions: [{
              topic: topicMetadata.name,
              count: numPartitions,
            }]
          });
        }
      }
    }
  } catch (err) {
    console.error('Error in ensureTopics:', err);
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
