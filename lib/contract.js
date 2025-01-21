'use strict';

const crypto = require('node:crypto');
const fsp = require('node:fs/promises');
const metautil = require('metautil');
const WebSocket = require('ws');
const { WebSocketServer } = WebSocket;

const createGenesisBlock = () => ({
  index: 0,
  previousHash: '0',
  transactions: [],
  timestamp: Date.now(),
  validator: 'genesis-node',
  hash: 'GENESIS',
});

const registry = new Map();

const chain = [createGenesisBlock()];

const addTransaction = (transaction) => {
  const latestBlock = chain[chain.length - 1];
  latestBlock.transactions.push(transaction);
};

const registerNode = (address, publicKey) => {
  if (!registry.has(address)) {
    registry.set(address, { publicKey, lastActive: Date.now() });
    addTransaction({ type: 'REGISTER_NODE', node: address, publicKey });
  }
};

const updateNodeActivity = (address) => {
  const node = registry.get(address);
  if (node) node.lastActive = Date.now();
};

const getNodes = () => Array.from(registry.keys());

// Storage

class FileStorage {
  constructor(basePath = './storage/') {
    this.records = new Map();
    this.basePath = basePath;
    return this.init();
  }

  async init() {
    await metautil.ensureDirectory(this.basePath);
    return this;
  }

  async saveData(id, data, encrypted = false) {
    const filePath = `${this.basePath}${id}.json`;
    const str = JSON.stringify({ data, encrypted });
    await fsp.writeFile(filePath, str);
    this.records.set(id, { path: filePath, encrypted });
  }

  async loadData(id) {
    const filePath = `${this.basePath}${id}.json`;
    const exists = fsp.access(filePath).then(...metautil.toBool);
    if (!exists) return [];
    const data = await fsp.readFile(filePath, { encoding: 'utf8' });
    return JSON.parse(data);
  }
}

// Crypto

const generateKeys = () => {
  const promise = new Promise((resolve, reject) => {
    const format = 'pem';
    const options = {
      modulusLength: 2048,
      publicKeyEncoding: { type: 'spki', format },
      privateKeyEncoding: { type: 'pkcs8', format },
    };
    const callback = (error, publicKey, privateKey) => {
      if (error) return void reject(error);
      resolve({ publicKey, privateKey });
    };
    crypto.generateKeyPair('rsa', options, callback);
  });
  return promise;
};

const encrypt = (data, publicKey) => {
  const buffer = Buffer.from(JSON.stringify(data));
  return crypto.publicEncrypt(publicKey, buffer).toString('base64');
};

const decrypt = (data, privateKey) => {
  const buffer = Buffer.from(data, 'base64');
  const str = crypto.privateDecrypt(privateKey, buffer).toString();
  return JSON.parse(str);
};

// Query

const checkCondition = (value, condition) => {
  if (typeof condition === 'string') {
    if (condition.includes('..')) {
      const [min, max] = condition.split('..').map(Number);
      return value >= min && value <= max;
    }
    const operators = ['<=', '>=', '<', '>', '='];
    for (const op of operators) {
      if (condition.startsWith(op)) {
        const number = parseFloat(condition.replace(op, '').trim());
        if (op === '<') return value < number;
        if (op === '>') return value > number;
        if (op === '<=') return value <= number;
        if (op === '>=') return value >= number;
        if (op === '=') return value === number;
      }
    }
    return value === condition;
  }
  if (Array.isArray(condition)) return condition.includes(value);
  if (condition.includes) return value.includes(condition.includes);
  if (condition.in) return condition.in.includes(value);
  return false;
};

const applyFilters = (collection, where) => {
  console.log({ filter: { collection, where } });
  const keys = Object.keys(where);
  const check = (record) => (key) => checkCondition(record[key], where[key]);
  return collection.data.filter((record) => keys.every(check(record)));
};

const applySorting = (collection, field, order) => {
  const compare = (a, b) => {
    const af = a[field];
    const bf = b[field];
    return order === 'desc' ? bf - af : af - bf;
  };
  return collection.sort(compare);
};

const applyGrouping = (collection, groupBy, aggregate) => {
  const groups = {};
  collection.forEach((record) => {
    const key = record[groupBy];
    if (!groups[key]) groups[key] = [];
    groups[key].push(record);
  });

  return Object.keys(groups).map((group) => {
    const aggregated = { [groupBy]: group };
    Object.keys(aggregate).forEach((field) => {
      const [operation, column] = aggregate[field].split(' ');
      if (operation === 'count') aggregated[field] = groups[group].length;
      if (operation === 'sum') {
        const reducer = (sum, rec) => sum + rec[column];
        aggregated[field] = groups[group].reduce(reducer, 0);
      }
    });
    return aggregated;
  });
};

class Query {
  constructor(storage) {
    this.storage = storage;
  }

  async execute(query) {
    let collection = await this.storage.loadData(query.find);
    if (!collection) return [];
    if (query.where) collection = applyFilters(collection, query.where);
    if (query.sortBy) {
      collection = applySorting(collection, query.sortBy, query.order);
    }
    if (query.groupBy) {
      return applyGrouping(collection, query.groupBy, query.aggregate);
    }
    if (query.limit) collection = collection.slice(0, query.limit);
    return collection;
  }
}

// Network

const feed = (ws, message) => {
  const { type, nodes } = JSON.parse(message);
  if (type === 'NODE:LIST') {
    for (const node of nodes) {
      registerNode(node, 'publicKey');
    }
  }
  if (type === 'NODE:PING') {
    const node = nodes[0];
    updateNodeActivity(node);
  }
};

class Peer {
  constructor(port, publicKey) {
    this.port = port;
    this.publicKey = publicKey;
    this.peers = new Set();

    this.server = new WebSocketServer({ port });
    this.server.on('connection', (ws) => this.handleConnection(ws));

    registerNode(`ws://localhost:${port}`, publicKey);
    this.connectToKnownNodes();
  }

  handleConnection(ws) {
    this.peers.add(ws);
    ws.on('message', (message) => feed(ws, message));
    ws.on('close', () => void this.peers.delete(ws));
    ws.send(JSON.stringify({ type: 'NODE:LIST', nodes: getNodes() }));
  }

  connectToKnownNodes() {
    for (const node of getNodes()) {
      this.connectToNode(node);
    }
  }

  connectToNode(node) {
    if (node === `ws://localhost:${this.port}`) return;
    const ws = new WebSocket(node);
    ws.on('open', () => {
      this.peers.add(ws);
      ws.send(JSON.stringify({ type: 'NODE:LIST', nodes: getNodes() }));
    });
    ws.on('message', (message) => feed(ws, message));
    ws.on('close', () => this.peers.delete(ws));
  }

  ping() {
    for (const ws of this.peers) {
      const node = `ws://localhost:${this.port}`;
      const packet = { type: 'NODE:PING', nodes: [node] };
      ws.send(JSON.stringify(packet));
    }
  }
}

module.exports = {
  addTransaction,
  registerNode,
  updateNodeActivity,
  getNodes,
  generateKeys,
  FileStorage,
  encrypt,
  decrypt,
  Peer,
  Query,
};
