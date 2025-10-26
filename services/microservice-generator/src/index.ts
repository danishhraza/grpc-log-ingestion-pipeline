import 'dotenv/config';
import path from 'path';
import protoLoader from '@grpc/proto-loader';
import grpc from '@grpc/grpc-js';
import { v4 as uuidv4 } from 'uuid';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PROTO_PATH = path.resolve(__dirname, '../../../proto/logging.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});

const proto = grpc.loadPackageDefinition(packageDefinition) as any;
const LogIngest = proto.logging.LogIngest;

const client = new LogIngest('localhost:50051', grpc.credentials.createInsecure());

function sendLog(serviceName: string) {
  const entry = {
    service: serviceName,
    instanceId: uuidv4(),
    timestamp: Date.now(),
    level: Math.random() < 0.98 ? 'INFO' : 'ERROR',
    message: `synthetic log from ${serviceName} @ ${new Date().toISOString()}`,
    attrs: { route: '/api/test' },
    latencyMs: Math.random() * 500
  };

  client.SendLog(entry, (err: any, res: any) => {
    if (err) {
      console.error('SendLog error:', err);
    } else {
      //console.log('ack', res);
    }
  });
}

// send burst of logs periodically
const services = ['auth-service', 'order-service', 'payment-service'];
setInterval(() => {
  // send random number of logs
  const n = Math.floor(Math.random() * 10) + 1;
  for (let i = 0; i < n; i++) {
    const svc = services[Math.floor(Math.random() * services.length)];
    sendLog(svc);
  }
}, 60000);

async function main() {
  try {
    console.log('microservice generator running — sending logs to collector on :50051');
  } catch (err) {
    console.error('Microservice generator failed:', err);
    process.exit(1);
  }
}

main();
