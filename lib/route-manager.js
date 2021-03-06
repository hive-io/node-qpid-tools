'use strict';
var Promise = require('bluebird'),
    amqp = require('amqp10'),
    BrokerAgent = require('qmf2'),
    retry = require('bluebird-retry'),
    debug = require('debug')('qpid:tools:route-manager');

function RouteManager(agent, broker, local) {
  this._agent = agent;
  this._broker = broker;
  this._local = local;
  this._pendingLinkRemovals = {};
}

RouteManager.prototype.listLinks = function() {
  return this._agent._getObjects('link');
};

RouteManager.prototype._getAndWaitForLink = function(remote) {
  var self = this;
  function waitForLink() {
    return self._agent.getAllLinks()
      .filter((link) => (link.host === remote.host && link.port === remote.port))
      .then((links) => {
        if (links.length === 0)
          throw new Error('no such link: local(' + self._local.host + ') => remote(' + remote.host + ')');
        debug('link[local(', self._local.host, ') => remote(', remote.host, ')], state: ', links[0].state);

        if (links[0].state.match(/Waiting|Connecting|Closing/i))
          throw new Error('link not ready');

        return links[0];
      });
  }

  return retry(waitForLink);
};

RouteManager.prototype._getLinksFor = function(remote) {
  if (remote === undefined || remote === null) throw new Error('missing `remote` argument');
  return this._agent.getAllLinks()
    .filter((link) => (link.host === remote.host && link.port === remote.port));
};

RouteManager.prototype.addLink = function(remote, options) {
  if (remote === undefined || remote === null) throw new Error('missing `remote` argument');
  options = Object.assign({}, { durable: false, transport: 'tcp' }, options);
  debug('adding link: local(', this._local.host, ') => remote(', remote.host, ')');

  return this._getLinksFor(remote)
    .then((links) => {
      if (links.length) {
        debug('link already exists for: ' + remote.host);
        return links[0];
      }

      let bridgeOptions = {
        host: remote.host, port: remote.port,
        username: remote.username || '', password: remote.password || '',
        durable: options.durable, authMechanism: '', transport: options.transport
      };

      return this._broker.connect(bridgeOptions)
        .then(() => this._getLinksFor(remote))
        .then((links) => links[0]);
    });
};

RouteManager.prototype.removeLink = function(remote) {
  if (remote === undefined || remote === null) throw new Error('missing `remote` argument');
  if (this._pendingLinkRemovals.hasOwnProperty(remote.host+remote.port)) return;

  debug('removing link: local(', this._local.host, ') => remote(', remote.host, ')');
  this._pendingLinkRemovals[remote.host+remote.port] = true;
  return this._getLinksFor(remote)
    .then((links) => {
      if (links.length === 0) {
        debug('no links found for: ' + remote.host);
        return;
      }

      return links[0].close();
    })
    .finally(() => { delete this._pendingLinkRemovals[remote.host+remote.port]; });
};

RouteManager.prototype.listRoutes = function(remote) {
  if (remote === undefined || remote === null) throw new Error('missing `remote` argument');
  debug('listing routes: local(', this._local.host, ') => remote(', remote.host, ')');
  return Promise.all([ this._agent._getObjects('link'), this._agent._getObjects('bridge') ])
    .spread((links, bridges) => {
      let result = [];
      bridges.forEach(bridge => {
        let foundLinks =
          links.filter(link => link._object_id._object_name === bridge.linkRef._object_name);
        if (!foundLinks.length) return;
        result.push({ link: foundLinks[0], bridge: bridge });
      });

      return result;
    });
};

RouteManager.prototype._getExistingRoutesForLink = function(remote, link) {
  if (remote === undefined || remote === null) throw new Error('missing `remote` argument');
  if (link === undefined || link === null) throw new Error('missing `link` argument');

  return this._agent._getObjects('bridge')
    .filter(bridge => bridge.linkRef === link._object_id);
};

RouteManager.prototype.addRoute = function(remote, exchange, options) {
  if (remote === undefined || remote === null) throw new Error('missing `remote` argument');
  if (exchange === undefined || exchange === null) throw new Error('missing `exchange` argument');
  options = Object.assign({}, { durable: false, dynamic: false }, options);
  debug('adding route: local(', this._local.host, ') => remote(', remote.host, '), exchange: ', exchange);

  let link;
  return this.addLink(remote, options) // first ensure a link exists
    .then(() => this._getAndWaitForLink(remote))
    .then((l) => {
      link = l;
      return this._getExistingRoutesForLink(remote, link);
    })
    .then(routes => {
      if (routes.length) {
        debug('duplicate route exists for link');
        return;
      }

      let bridgeOptions = {
        durable: options.durable, src: exchange, dest: exchange,
        key: '', tag: '', excludes: '', srcIsQueue: false, srcIsLocal: false,
        dynamic: options.dynamic, sync: 0, credit: 0
      };

      return link.bridge(bridgeOptions);
    });
};

RouteManager.prototype.removeRoute = function(remote, exchange, options) {
  if (remote === undefined || remote === null) throw new Error('missing `remote` argument');
  if (exchange === undefined || exchange === null) throw new Error('missing `exchange` argument');
  options = Object.assign({}, { deleteEmptyLinks: true }, options);
  debug('removing route: local(', this._local.host, ') => remote(', remote.host, '), exchange: ', exchange);

  return this._getAndWaitForLink(remote)
    .then((link) => {
      if (!link) {
        console.log('no link exists for: ', remote.host);
        return;
      }

      return this._getExistingRoutesForLink(remote, link)
        .filter(bridge => bridge.dest === exchange)
        .map(bridge => bridge.close())
        .then(() => options.deleteEmptyLinks ? this.removeLink(remote) : null);
    });
};

RouteManager.prototype.clearRoutes = function(options) {
  options = Object.assign({}, { deleteEmptyLinks: true }, options);
  return this._agent._getObjects('bridge')
    .map(bridge => {
      debug('deleting bridge: ', this._local.host, ':', this._local.port, ' ', bridge.dest, ' ', bridge.key);
      return bridge.close();
    })
    .then(() => {
      if (options.deleteEmptyLinks) {
        return this._agent._getObjects('link')
          .map(link => {
            debug('deleting link: ', link.host, ':', link.port);
            return link.close();
          });
      }
    });
};

module.exports = function(local) {
  let policy = amqp.Policy.merge({
    reconnect: { retries: 5, forever: false }
  }, amqp.Policy.DefaultPolicy);

  let client = new amqp.Client(policy),
      agent = new BrokerAgent(client);

  return client.connect(local.connectionString)
    .then(() => agent.initialize())
    .then(() => agent.getAllBrokers())
    .then((brokers) => {
      return new RouteManager(agent, brokers[0], local);
    });
};
