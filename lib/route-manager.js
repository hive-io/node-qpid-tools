'use strict';
var Promise = require('bluebird'),
    amqp = require('amqp10'),
    BrokerAgent = require('qmf2'),
    retry = require('bluebird-retry');

function RouteManager(agent, broker, local) {
  this._agent = agent;
  this._broker = broker;
  this._local = local;
}

RouteManager.prototype._getAndWaitForLink = function(remote) {
  var self = this;
  function waitForLink() {
    return self._agent.getAllLinks()
      .filter((link) => (link.host === remote.host && link.port === remote.port))
      .then((links) => {
        if (links.length === 0)
          throw new Error('no such link: local(' + self._local.host + ') => remote(' + remote.host + ')');
        console.log('link[local(', self._local.host, ') => remote(', remote.host, ')], state: ', links[0].state);

        if (links[0].state.match(/Waiting|Connecting|Closing/i))
          throw new Error('link not ready');

        return links[0];
      });
  }

  return retry(waitForLink);
};

RouteManager.prototype._getLinksFor = function(remote) {
  return this._agent.getAllLinks()
    .filter((link) => (link.host === remote.host && link.port === remote.port));
};

RouteManager.prototype.addLink = function(remote) {
  console.log('adding link: local(', this._local.host, ') => remote(', remote.host, ')');

  return this._getLinksFor(remote)
    .then((links) => {
      if (links.length) {
        console.log('link already exists for: ' + remote.host);
        return links[0];
      }

      var bridgeOptions = {
        host: remote.host, port: remote.port,
        username: remote.username || '', password: remote.password || '',
        durable: false, authMechanism: '', transport: 'tcp'
      };

      return this._broker.connect(bridgeOptions)
        .then(() => this._getLinksFor(remote))
        .then((links) => links[0]);
    });
};

RouteManager.prototype.removeLink = function(remote) {
  console.log('removing link: local(', this._local.host, ') => remote(', remote.host, ')');

  return this._getLinksFor(remote)
    .then((links) => {
      if (links.length === 0) {
        console.log('no links found for: ' + remote.host);
        return;
      }

      return links[0].close();
    });
};

RouteManager.prototype.listRoutes = function(remote) {
  console.log('listing routes: local(', this._local.host, ') => remote(', remote.host, ')');

  return Promise.all([ this._agent._getObjects('link'), this._agent._getObjects('bridge') ])
    .spread((links, bridges) => {
      var result = [];
      bridges.forEach(bridge => {
        var foundLinks = links.filter(link => link._object_id._object_name === bridge.linkRef._object_name);
        if (!foundLinks.length) return;
        result.push({ link: foundLinks[0], bridge: bridge });
      });

      return result;
    });
};

RouteManager.prototype._getExistingRoutesForLink = function(remote, link) {
  return this._agent._getObjects('bridge')
    .filter(bridge => bridge.linkRef === link._object_id);
};

RouteManager.prototype.addRoute = function(remote, exchange, options) {
  options = options || {};
  console.log('adding route: local(', this._local.host, ') => remote(', remote.host, '), exchange: ', exchange);

  var link;
  return this.addLink(remote) // first ensure a link exists
    .then(() => this._getAndWaitForLink(remote))
    .then((l) => {
      link = l;
      return this._getExistingRoutesForLink(remote, link);
    })
    .then(routes => {
      if (routes.length) {
        console.log('duplicate route exists for link');
        return;
      }

      return link.bridge({
        durable: options.durable || false, src: exchange, dest: exchange,
        key: '', tag: '', excludes: '', srcIsQueue: false, srcIsLocal: false,
        dynamic: options.dynamic || true, sync: 0, credit: 0
      });
    });
};

RouteManager.prototype.removeRoute = function(remote, exchange) {
  console.log('removing route: local(', this._local.host, ') => remote(', remote.host, '), exchange: ', exchange);

  return this._getAndWaitForLink(remote)
    .then((link) => {
      if (!link) {
        console.log('no link exists for: ', remote.host);
        return;
      }

      return this._getExistingRoutesForLink(remote, link)
        .filter(bridge => bridge.dest === exchange)
        .map(bridge => bridge.close())
        .then(() => this.removeLink(remote));
    });
};

module.exports = function(local) {
  var client = new amqp.Client(),
      agent = new BrokerAgent(client);

  return client.connect(local.connectionString)
    .then(() => agent.initialize())
    .then(() => agent.getAllBrokers())
    .then((brokers) => {
      return new RouteManager(agent, brokers[0], local);
    });
};
