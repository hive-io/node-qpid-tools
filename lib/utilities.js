'use strict';
var url = require('url'),
    u = module.exports = {};

u.parseBrokerAddress = function(address) {
  if (address.indexOf('//') === -1) address = '//' + address;
  var parsed = url.parse(address, false, true);
  parsed.auth = parsed.auth || '';
  parsed.port = !!parsed.port ? parseInt(parsed.port) : 5672;
  var cs =
    ((parsed.port === 5672) ? 'amqp://' : 'amqps://') +
    (!!parsed.auth ? parsed.auth + '@' : '') +
    parsed.hostname + ':' + parsed.port;

  return {
    host: parsed.hostname, port: !!parsed.port ? parseInt(parsed.port) : 5672,
    username: parsed.auth.split(':')[0] || '', password: parsed.auth.split(':')[1] || '',
    connectionString: cs
  };
};
