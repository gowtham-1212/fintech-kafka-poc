import { getProducer, getConsumer, disconnectKafka, ensureTopics } from './kafka.js';

async function processPayment(event: any) {
  const producer = await getProducer();
  
  // Simulate business logic delay (1-3 seconds)
  const delay = Math.floor(Math.random() * 2000) + 1000;
  await new Promise(resolve => setTimeout(resolve, delay));

  // Simulate success/fail (80% success)
  const isSuccess = Math.random() > 0.2;
  const status = isSuccess ? 'completed' : 'failed';
  
  const resultEvent = {
    ...event,
    status,
    processedAt: new Date().toISOString(),
    failureReason: isSuccess ? null : 'Insufficient funds or simulated error',
  };

  await producer.send({
    topic: `payment.${status}`,
    messages: [
      { value: JSON.stringify(resultEvent) },
    ],
  });

  console.log(`Processed transaction ${event.transactionId}: ${status}`);
}

const start = async () => {
  // Ensure required topics exist before subscribing
  await ensureTopics(['payment.initiated', 'payment.completed', 'payment.failed']);
  
  const consumer = await getConsumer();
  
  await consumer.subscribe({ topic: 'payment.initiated', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      if (!message.value) return;
      
      const payload = JSON.parse(message.value.toString());
      console.log(`Received payment initiation: ${payload.transactionId}`);
      
      await processPayment(payload);
    },
  });
};

const gracefulShutdown = async () => {
  console.log('Shutting down gracefully...');
  await disconnectKafka();
  process.exit(0);
};

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

start().catch(err => {
  console.error('Error starting transaction engine:', err);
  process.exit(1);
});
