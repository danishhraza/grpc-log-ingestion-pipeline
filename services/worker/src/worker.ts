import 'dotenv/config';
import amqp from 'amqplib';
import { Client } from '@elastic/elasticsearch';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const RABBIT_URL = process.env.RABBIT_URL || 'amqp://localhost:5672';
const QUEUE_NAME = 'logs_queue';
const ES_URL = process.env.ES_URL || 'http://localhost:9200';
const INDEX_PREFIX = 'logs';

const es = new Client({ node: ES_URL });

async function ensureIndexTemplate() {
  const mappingPath = path.resolve(__dirname, '../../../infrastructure/es-mapping.json');
  if (fs.existsSync(mappingPath)) {
    const mapping = JSON.parse(fs.readFileSync(mappingPath, 'utf8'));
    
    // Create an index template for logs-* pattern
    const templateName = 'logs-template';
    try {
      await es.indices.putIndexTemplate({
        name: templateName,
        index_patterns: ['logs-*'],
        template: {
          ...mapping,
          settings: {
            number_of_shards: 1,
            number_of_replicas: 0
          }
        }
      });
      console.log(`✅ Index template '${templateName}' created/updated successfully`);
    } catch (error) {
      console.error('Failed to create index template:', error);
      throw error;
    }
  } else {
    console.warn('No mapping file found at', mappingPath);
    throw new Error('Mapping file is required');
  }
}

async function ensureDailyIndex(indexName: string) {
  try {
    const exists = await es.indices.exists({ index: indexName });
    if (!exists) {
      await es.indices.create({ index: indexName });
      console.log(`✅ Created index: ${indexName}`);
    }
  } catch (error) {
    console.error(`Failed to ensure index ${indexName}:`, error);
    throw error;
  }
}

async function bulkIndex(docs: any[]) {
  if (docs.length === 0) return;
  
  // Group documents by index to ensure indices exist
  const indexGroups = new Map<string, any[]>();
  
  for (const doc of docs) {
    try {
      // --- FIXED TIMESTAMP PARSING ---
      let timestampValue = doc.timestamp;

      // If it's a numeric string (e.g. "1761491694304"), convert to number
      if (typeof timestampValue === 'string' && /^\d+$/.test(timestampValue)) {
        timestampValue = Number(timestampValue);
      }

      let timestamp;
      if (typeof timestampValue === 'number') {
        // Handle both seconds and milliseconds
        const ts = timestampValue > 1e10 ? timestampValue : timestampValue * 1000;
        timestamp = new Date(ts);
      } else if (typeof timestampValue === 'string') {
        timestamp = new Date(timestampValue);
      } else {
        console.warn('Invalid timestamp format, using current time:', doc.timestamp);
        timestamp = new Date();
      }

      // Fallback if still invalid
      if (isNaN(timestamp.getTime())) {
        console.warn('Invalid timestamp value, using current time:', doc.timestamp);
        timestamp = new Date();
      }

      // --- END FIX ---

      // Create a daily index (e.g. logs-2025-10-26)
      const idxName = `${INDEX_PREFIX}-${timestamp.toISOString().slice(0, 10)}`;
      
      // Store timestamp in ISO format for Kibana
      const esDoc = { ...doc, timestamp: timestamp.toISOString() };
      
      if (!indexGroups.has(idxName)) {
        indexGroups.set(idxName, []);
      }
      indexGroups.get(idxName)!.push(esDoc);

    } catch (err) {
      console.error('Error processing document:', doc, err);
      continue;
    }
  }

  // Ensure all required indices exist
  for (const indexName of indexGroups.keys()) {
    await ensureDailyIndex(indexName);
  }

  // Build bulk request body
  const body: any[] = [];
  for (const [indexName, indexDocs] of indexGroups) {
    for (const doc of indexDocs) {
      // Use 'create' instead of 'index' to avoid the data stream conflict
      body.push({ create: { _index: indexName, _id: doc.id } });
      body.push(doc);
    }
  }

  try {
    const resp = await es.bulk({ refresh: true, body });
    if (resp.errors) {
      console.error('Bulk had errors', JSON.stringify(resp.items, null, 2));
    } else {
      console.log(`✅ Bulk indexed ${docs.length} docs`);
    }
  } catch (err) {
    console.error('ES bulk error', err);
    throw err;
  }
}


async function run() {
  await ensureIndexTemplate();

  const conn = await amqp.connect(RABBIT_URL);
  const channel = await conn.createChannel();
  await channel.assertQueue(QUEUE_NAME, { durable: true });
  channel.prefetch(100); // tune batch size

  const buffer: any[] = [];
  const BUFFER_LIMIT = 500;
  const FLUSH_INTERVAL_MS = 2000;
  let flushing = false;

  const flush = async () => {
    if (flushing || buffer.length === 0) return;
    flushing = true;
    const toIndex = buffer.splice(0, buffer.length);
    try {
      await bulkIndex(toIndex);
    } catch (err) {
      console.error('Failed to bulk index, pushing back to queue', err);
      // If indexing fails, requeue items (simple approach)
      for (const d of toIndex) {
        channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(d)), { persistent: true });
      }
    } finally {
      flushing = false;
    }
  };

  setInterval(flush, FLUSH_INTERVAL_MS);

  console.log('Worker waiting for messages...');
  channel.consume(QUEUE_NAME, async (msg) => {
    if (!msg) return;
    try {
      const doc = JSON.parse(msg.content.toString());
      // Log the document to debug timestamp issues
      console.log('Received document:', JSON.stringify(doc, null, 2));
      buffer.push(doc);

      // ack immediately to avoid re-delivery during heavy load, BUT ensure durable indexing strategy in production
      channel.ack(msg);

      if (buffer.length >= BUFFER_LIMIT) {
        await flush();
      }
    } catch (err) {
      console.error('Message processing error', err);
      channel.nack(msg, false, false); // send to DLQ or drop depending on configuration
    }
  }, { noAck: false });
}

async function main() {
  try {
    console.log('Starting worker...');
    await run();
  } catch (err) {
    console.error('Worker failed:', err);
    process.exit(1);
  }
}

main();
