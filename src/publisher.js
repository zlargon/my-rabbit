#!/usr/bin/env node

const amqp = require('amqplib');
const config = require('./config');
const delay = require('./delay');

const publish = (channel, exchangeName, rountingKey, message) => new Promise(resolve => {
  channel.publish(
    exchangeName,
    rountingKey,
    new Buffer(message), {
      contentType: 'application/json',
      contentEncoding: 'utf-8',
      persistent: true
    },
    (err) => {
      if (err) {
        resolve(err); // NACK? untestable
      } else {
        resolve();    // ACK
      }
    }
  );
});

(async () => {

  const {
    url,
    exchangeName,
    rountingKey,
    retryInterval
  } = config;

  let taskIndex = 0;
  const taskList = new Array(50).fill(0).map((x, i) => {
    return {
      message: {
        action: 'ACTION',
        payload: {
          id: i
        }
      },
      rountingKey: rountingKey
    }
  });

  for (;;) {
    try {
      const connection = await amqp.connect(url);
      const channel = await connection.createConfirmChannel();

      // 1. create exchange
      const exchangeType = 'topic';
      await channel.assertExchange(exchangeName, exchangeType, {
        durable: true
      });
      // { exchange: 'topic_logs' }

      // Event Handler
      // channel.on('error',  (err) => {...})
      // channel.on('close',  ()    => {...})
      // channel.on('return', (msg) => {...})
      // channel.on('drain',  ()    => {...})

      // 2. publish message
      for ( ; taskIndex < taskList.length; taskIndex++) {
        const task = taskList[taskIndex];
        const msg = JSON.stringify(task.message);

        const err = await publish(channel, exchangeName, task.rountingKey, msg);
        if (err) {
          console.log(`send '${msg}' failed`);  // NACK? untestable
          taskIndex--;
        } else {
          console.log(`send '${msg}' sucess`);  // ACK
        }

        // await delay(1000);
      }

      // close channel and connectoin when all publish confirm
      await channel.waitForConfirms();
      console.log('all publish are confirm');

      await channel.close();
      await connection.close();
      break;

    } catch (e) {
      // console.log(e.message);
      console.log(e.stack);
      // console.log(e.stackAtStateChange);

      console.log(`Reconnect ${url} after ${retryInterval} seconds`);
      await delay(retryInterval * 1000);
    }
  }
})();
