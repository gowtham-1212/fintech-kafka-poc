import Fastify from 'fastify';
import { getProducer, disconnectProducer, ensureTopics } from './kafka.js';

const fastify = Fastify({ logger: true });

interface PaymentRequest {
  userId: string;
  amount: number;
  receiverId: string;
}

fastify.post('/api/pay', async (request, reply) => {
  const body = request.body as PaymentRequest;
  
  if (!body.userId || !body.amount || !body.receiverId) {
    return reply.status(400).send({ error: 'Missing required fields' });
  }

  const producer = await getProducer();
  
  const event = {
    transactionId: crypto.randomUUID(),
    ...body,
    status: 'initiated',
    timestamp: new Date().toISOString(),
  };

  await producer.send({
    topic: 'payment.initiated',
    messages: [
      { value: JSON.stringify(event) },
    ],
  });

  fastify.log.info(`Payment initiated: ${event.transactionId}`);
  
  return {
    success: true,
    transactionId: event.transactionId,
    message: 'Payment initiation event sent to Kafka',
  };
});

const start = async () => {
  try {
    // Ensure required topics exist before the gateway starts producing
    await ensureTopics(['payment.initiated', 'payment.completed', 'payment.failed']);
    
    const port = parseInt(process.env.PORT || '3001');
    await fastify.listen({ port, host: '0.0.0.0' });
    console.log(`Payment Gateway listening on port ${port}`);
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

const gracefulShutdown = async () => {
  console.log('Shutting down gracefully...');
  await fastify.close();
  await disconnectProducer();
  process.exit(0);
};

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

start();
