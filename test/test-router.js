'use strict';
var Promise = require('bluebird'),
    routeManager = require('..').RouteManager,
    url = require('url');

function parseBrokerAddress(address) {
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
}

var local = parseBrokerAddress('system:manager@localhost:5672'),
    remote = parseBrokerAddress('system:manager@localhost:5672'),
    exchange = 'amq.topic';

return Promise.all([ routeManager(local), routeManager(remote) ]).bind({})
  .spread((localRouter, remoteRouter) => {
    this.localRouter = localRouter;
    this.remoteRouter = remoteRouter;
    return localRouter.listRoutes(remote);
  })
  .then((routes) => {
    var hasRoute = routes.some(route => route.bridge.dest === exchange);
    if (hasRoute) {
      console.log('removing existing dynamic routes for: ', exchange);
      return Promise.all([
        this.localRouter.removeRoute(remote, exchange),
        this.remoteRouter.removeRoute(local, exchange)
      ]);
    }
  })
  .then(() => this.localRouter.clearRoutes())
  .then(() => Promise.all([
    this.localRouter.addRoute(remote, exchange), this.remoteRouter.removeRoute(local, exchange)]))
  .catch((err) => console.log('error: ', err))
  .then(() => process.exit(0));

