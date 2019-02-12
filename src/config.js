const config = {
  url:          'amqp://localhost',
  exchangeName: 'topic_logs',
  rountingKey:  'test',
  queueName:    'test_queue', // for consumer only
  retryInterval: 5
};

module.exports = config;
