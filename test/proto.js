'use strict';

const assert = require('node:assert');
const test = require('node:test');
const metacontracts = require('..');
const { FileStorage, generateKeys, Query, Peer } = metacontracts;

test('Stub', async () => {
  const { privateKey, publicKey } = await generateKeys();
  console.log({ privateKey, publicKey });
  assert.ok({ privateKey, publicKey });

  const storage = await new FileStorage();
  console.log({ storage });
  const query = new Query(storage);

  const node1 = new Peer(8081, publicKey);
  assert.ok(node1);
  const node2 = new Peer(8082, publicKey);
  assert.ok(node2);

  await storage.saveData('users', [
    { id: 1, name: 'Marcus', age: 25, salary: 20000, debt: -50 },
    { id: 2, name: 'Lucius', age: 44, salary: 30000, debt: -200 },
  ]);
  const users = await query.execute({
    find: 'users',
    where: { age: '18..30', salary: '>10000' },
  });
  console.log({ users });
});
