import { getProducer, getConsumer, disconnectKafka, ensureTopics } from './kafka.js';
import { connectRedis, redisClient } from './redis.js';

interface Metrics {
  totalVolume: number;
  initiatedCount: number;
  completedCount: number;
  failedCount: number;
  lastUpdate: string;
}

const METRICS_KEY = 'payment_metrics';

async function getStoredMetrics(): Promise<Metrics> {
  const stored = await redisClient.get(METRICS_KEY);
  if (stored) {
    return JSON.parse(stored);
  }
  return {
    totalVolume: 0,
    initiatedCount: 0,
    completedCount: 0,
    failedCount: 0,
    lastUpdate: new Date().toISOString(),
  };
}

async function persistMetrics(metrics: Metrics) {
  await redisClient.set(METRICS_KEY, JSON.stringify(metrics));
}

async function broadcastMetrics(metrics: Metrics) {
  const producer = await getProducer();
  metrics.lastUpdate = new Date().toISOString();
  
  await producer.send({
    topic: 'analytics.metrics',
    messages: [
      { value: JSON.stringify(metrics) },
    ],
  });
}

const start = async () => {
  // Connect to Redis
  await connectRedis();

  await ensureTopics(['analytics.metrics']);
  
  const consumer = await getConsumer();
  
  // Initialize metrics from Redis or default
  let metrics = await getStoredMetrics();
  console.log('Initial metrics from Redis:', metrics);

  await consumer.subscribe({ 
    topics: ['payment.initiated', 'payment.completed', 'payment.failed'], 
    fromBeginning: true 
  });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (!message.value) return;
      
      const payload = JSON.parse(message.value.toString());
      
      switch (topic) {
        case 'payment.initiated':
          metrics.initiatedCount++;
          metrics.totalVolume += payload.amount;
          break;
        case 'payment.completed':
          metrics.completedCount++;
          break;
        case 'payment.failed':
          metrics.failedCount++;
          break;
      }
      
      // 1. Persist to Redis
      await persistMetrics(metrics);
      
      // 2. Broadcast update to Kafka
      await broadcastMetrics(metrics);
    },
  });
};

const gracefulShutdown = async () => {
  console.log('Shutting down gracefully...');
  await disconnectKafka();
  await redisClient.disconnect();
  process.exit(0);
};

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

start().catch(err => {
  console.error('Analytics Service Error:', err);
  process.exit(1);
});
