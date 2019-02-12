const delay = require('./delay');

async function messageHandler (action, payload) {
  console.log(`action: ${action}`);
  console.log(`payload: ${JSON.stringify(payload)}`);
  await delay(500);
}

module.exports = messageHandler;
