#!/usr/bin/env node
const amqp = require('amqplib');
const config = require('./config');
const delay = require('./delay');
const messageHandler = require('./messageHandler');

(async function main () {
  const { url, exchangeName, rountingKey, queueName, retryInterval } = config;

  let connection;
  for (;;) {
    try {
      connection = await amqp.connect(url);
      console.log(`Connect ${url} successfully`);
      break;
    } catch (e) {
      console.log(`Reconnect ${url} after ${retryInterval} seconds`);
      await delay(retryInterval * 1000);
    }
  }

  async function errorHandler(err, type) {

    console.log(`\nERROR TYPE: ${type}`);

    if (err !== null) {
      // show error message
      console.log(err.message);
      console.log(err.stack);
      console.log('');
    }

    // close connection
    try {
      await connection.close();
    } catch (e) {
      console.log('\nERROR TYPE: RECLOSE');
    }

    console.log(`Restart the consumer after ${retryInterval} seconds`);
    await delay(retryInterval * 1000);
    await main();
  }

  try {
    const channel = await connection.createChannel();

    // setup channel:
    // fetch 1 task at one time
    // 'channel.prefetch' must be setup before 'channel.consume'
    channel.prefetch(1);

    // Event Handler
    // (1) error
    channel.on('error', (e) => {
      errorHandler(e, 'EVENT_ERROR');
    });

    // (2) close
    channel.on('close', () => {
      // 'close' will also be emitted, after 'error'
      errorHandler(null, 'EVENT_CLOSE');
    });

    // (3) drain
    channel.on('drain', () => {
      errorHandler(null, 'EVENT_DRAIN');
    });

    // (4) return
    // channel.on('return', (msg) => {...})


    // 1. define topic exchange
    const exchangeType = 'topic';
    const exchange = await channel.assertExchange(exchangeName, exchangeType, {
      durable: true
    });
    // exchange = { exchange: 'topic_logs' }

    // 2. define queue
    const queue = await channel.assertQueue(queueName, {
      durable: true // (1) mark both the queue and messages as durable. // TODO: configurable
    });
    // queue = { queue: 'amq.gen-oVNVsXbdczTwaZlSzOblzw', messageCount: 0, consumerCount: 0 }

    // 3. bind exchange and queue
    const bind = await channel.bindQueue(queue.queue, exchange.exchange, rountingKey);
    // bind = {}

    // 4. define messages receiver
    const consumer = await channel.consume(queue.queue, async (messageInfo) => {
      // consumer = {
      //   content: <Buffer .. .. ..>,
      //   fields: {
      //     consumerTag: 'amq.ctag-XoZPcmjCK1wPavhZEMTKAw',
      //     deliveryTag: 15,
      //     redelivered: false,
      //     exchange: 'topic_logs',
      //     routingKey: 'test'
      //   },
      //   properties: {
      //     contentType: 'application/json',
      //     contentEncoding: 'utf-8',
      //     headers: {},
      //     deliveryMode: 2,
      //     priority: undefined,
      //     correlationId: undefined,
      //     replyTo: undefined,
      //     expiration: undefined,
      //     messageId: undefined,
      //     timestamp: undefined,
      //     type: undefined,
      //     userId: undefined,
      //     appId: undefined,
      //     clusterId: undefined
      //   }
      // }

      try {
        // parse JSON data
        // This should not be throw exception because it assumed to be checked on web server
        const { action, payload } = JSON.parse(messageInfo.content.toString());

        // get the result of messageHandler
        const result = await messageHandler(action, payload);

        // ACK if return nothing
        if (typeof result === 'undefined') {
          channel.ack(messageInfo);
        } else {

          // REJECT if return { requeue: true/false }
          const { requeue } = result;
          channel.reject(messageInfo, requeue);
        }

      } catch (e) {
        // consumer level error handler
        errorHandler(e, 'CONSUMER');
      }
    });
    // consumer = { consumerTag: 'amq.ctag-fHU2KeobP873cB-Ak_CLWQ' }

  } catch (e) {
    // connection and channel level error handler
    errorHandler(e, 'OTHER');
  }
})();
