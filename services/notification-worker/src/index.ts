import Fastify from 'fastify';
import cors from '@fastify/cors';
import { getConsumer, disconnectKafka, ensureTopics } from './kafka.js';
import { EventEmitter } from 'events';

const fastify = Fastify({ logger: true });
const eventEmitter = new EventEmitter();

// Increase listeners limit for eventEmitter
eventEmitter.setMaxListeners(100);

// Support CORS for frontend
fastify.register(cors, {
  origin: '*',
  methods: ['GET'],
});

fastify.get('/api/stream', (request, reply) => {
  console.log('New SSE client connected');
  
  reply.raw.setHeader('Content-Type', 'text/event-stream');
  reply.raw.setHeader('Cache-Control', 'no-cache, no-transform');
  reply.raw.setHeader('Connection', 'keep-alive');
  reply.raw.setHeader('X-Accel-Buffering', 'no'); // Disable buffering for Nginx/Proxies
  reply.raw.setHeader('Access-Control-Allow-Origin', '*');

  // Send initial retry period
  reply.raw.write('retry: 5000\n\n');
  
  // Keep-alive heartbeat every 15 seconds
  const keepAlive = setInterval(() => {
    reply.raw.write(': heartbeat\n\n');
  }, 15000);

  const onEvent = (data: any) => {
    console.log('Sending event to client:', data.transactionId);
    reply.raw.write(`data: ${JSON.stringify(data)}\n\n`);
  };

  eventEmitter.on('kafka-event', onEvent);

  request.raw.on('close', () => {
    console.log('SSE client disconnected');
    clearInterval(keepAlive);
    eventEmitter.removeListener('kafka-event', onEvent);
  });
});

const startKafkaConsumer = async () => {
  await ensureTopics(['payment.initiated', 'payment.completed', 'payment.failed', 'analytics.metrics']);
  
  const consumer = await getConsumer();
  
  await consumer.subscribe({ 
    topics: ['payment.initiated', 'payment.completed', 'payment.failed', 'analytics.metrics'], 
    fromBeginning: true 
  });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (!message.value) return;
      
      try {
        const payload = JSON.parse(message.value.toString());
        
        // Broadcast both transaction events and analytics metrics
        eventEmitter.emit('kafka-event', {
          topic,
          ...payload
        });
      } catch (err) {
        console.error('Error parsing Kafka message:', err);
      }
    },
  });
};

const start = async () => {
  try {
    const port = parseInt(process.env.PORT || '3003');
    
    // Start Kafka consumer
    await startKafkaConsumer().catch(err => {
      console.error('Kafka Consumer Fatal Error:', err);
    });

    await fastify.listen({ port, host: '0.0.0.0' });
    console.log(`Notification Worker listening on port ${port}`);
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

const gracefulShutdown = async () => {
  console.log('Shutting down gracefully...');
  await fastify.close();
  await disconnectKafka();
  process.exit(0);
};

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

start();
