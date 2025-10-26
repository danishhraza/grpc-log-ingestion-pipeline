import 'dotenv/config';
import path from 'path';
import protoLoader from '@grpc/proto-loader';
import grpc from '@grpc/grpc-js';
import amqp from 'amqplib';
import { v4 as uuidv4 } from 'uuid';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PROTO_PATH = path.resolve(__dirname, '../../../proto/logging.proto');
const RABBIT_URL = process.env.RABBIT_URL || 'amqp://localhost:5672';
const QUEUE_NAME = 'logs_queue';

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});

const proto = grpc.loadPackageDefinition(packageDefinition) as any;
const LogIngestService = proto.logging.LogIngest.service;

let channel: amqp.Channel | null = null;

async function startRabbit() {
  const conn = await amqp.connect(RABBIT_URL);
  channel = await conn.createChannel();
  // durable queue
  await channel.assertQueue(QUEUE_NAME, { durable: true });
  console.log('Connected to RabbitMQ.');
}

const server = new grpc.Server();

const handlers = {
  SendLog: async (call: any, callback: any) => {
    try {
      const entry = call.request;
      const id = uuidv4();
      const payload = { ...entry, id };
      // publish to queue
      if (!channel) throw new Error('RabbitMQ channel not ready');
      channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(payload)), { persistent: true });
      callback(null, { ok: true, id });
    } catch (err) {
      console.error('SendLog error', err);
      callback(err);
    }
  },

  SendLogs: async (stream: any, callback: any) => {
    try {
      stream.on('data', (entry: any) => {
        const id = uuidv4();
        const payload = { ...entry, id };
        if (!channel) {
          console.error('Channel not ready for stream');
          return;
        }
        channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(payload)), { persistent: true });
      });

      stream.on('end', () => {
        callback(null, { ok: true, id: 'stream' });
      });

    } catch (err) {
      console.error('SendLogs error', err);
      callback(err);
    }
  }
};

async function main() {
  await startRabbit();

  server.addService(LogIngestService, handlers);
  const addr = '0.0.0.0:50051';
  server.bindAsync(addr, grpc.ServerCredentials.createInsecure(), (err: any, port: number) => {
    if (err) {
      console.error('Server bind error', err);
      process.exit(1);
    }
    server.start();
    console.log(`Collector gRPC server listening on ${addr}`);
  });
}

main().catch(err => {
  console.error('Fatal', err);
  process.exit(1);
});
