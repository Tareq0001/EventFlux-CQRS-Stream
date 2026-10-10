const assert = require('assert');
const { EventStore } = require('../src/infrastructure/event-store');
const { ProjectionEngine } = require('../src/infrastructure/projection-engine');
const { OutboxProcessor } = require('../src/infrastructure/outbox-processor');
const { CommandBus } = require('../src/application/command-bus');
const { OrderAggregate } = require('../src/domain/aggregate');

console.log('--- RUNNING EVENTFLUX CQRS ARCHITECTURE SUITE ---');

// Test 1: Order Aggregate Lifecycle & Invariant Validation
{
  const order = new OrderAggregate('ord_101');
  assert.strictEqual(order.status, 'INITIAL');

  order.createOrder('cust_42', [{ name: 'Enterprise SSD', price: 250 }], 250);
  assert.strictEqual(order.status, 'PENDING');
  assert.strictEqual(order.totalAmount, 250);
  assert.strictEqual(order.uncommittedEvents.length, 1);

  // Invariant: Cannot pay with insufficient amount
  assert.throws(() => order.payOrder('pay_ref_99', 100), /Insufficient payment/);

  // Pay valid amount
  order.payOrder('pay_ref_99', 250);
  assert.strictEqual(order.status, 'PAID');
  assert.strictEqual(order.uncommittedEvents.length, 2);

  // Invariant: Cannot cancel paid and shipped order
  order.shipOrder('TRK-998877', 'DHL-Express');
  assert.strictEqual(order.status, 'SHIPPED');
  assert.throws(() => order.cancelOrder('Changed mind'), /Cannot cancel order that has already been shipped/);

  console.log('✓ Test 1 Passed: Order Aggregate Lifecycle & Business Invariants Verified');
}

// Test 2: Event Rehydration (State reconstruction from history)
{
  const store = new EventStore();
  const outbox = new OutboxProcessor();
  const bus = new CommandBus(store, outbox);

  bus.execute({
    type: 'CreateOrderCommand',
    orderId: 'ord_202',
    customerId: 'cust_88',
    items: [{ name: 'Cloud License', price: 1200 }],
    totalAmount: 1200
  });

  bus.execute({
    type: 'PayOrderCommand',
    orderId: 'ord_202',
    paymentReference: 'PAY_WIRE_55',
    paidAmount: 1200
  });

  // Reconstitute from empty object
  const rehydrated = new OrderAggregate('ord_202');
  const events = store.getEventsForAggregate('ord_202');
  rehydrated.loadFromHistory(events);

  assert.strictEqual(rehydrated.status, 'PAID');
  assert.strictEqual(rehydrated.totalAmount, 1200);
  assert.strictEqual(rehydrated.version, 2);
  console.log('✓ Test 2 Passed: Event Sourcing Aggregate Rehydration Verified');
}

// Test 3: Optimistic Concurrency Control (OCC Conflict Detection)
{
  const store = new EventStore();
  const { OrderCreatedEvent } = require('../src/domain/event');

  const evt1 = new OrderCreatedEvent('ord_303', 'c1', [{ name: 'Item A', price: 50 }], 50);
  store.appendEvents('ord_303', [evt1], 0);

  // Second append expecting version 0 should throw conflict because version is now 1
  const evt2 = new OrderCreatedEvent('ord_303', 'c1', [{ name: 'Item B', price: 50 }], 50);
  assert.throws(() => {
    store.appendEvents('ord_303', [evt2], 0);
  }, /ConcurrencyConflict/);

  console.log('✓ Test 3 Passed: Optimistic Concurrency Control Conflict Guard Verified');
}

// Test 4: CQRS Projection Engine Materialized Views
{
  const store = new EventStore();
  const projections = new ProjectionEngine(store);
  const bus = new CommandBus(store, null);

  bus.execute({
    type: 'CreateOrderCommand',
    orderId: 'ord_404',
    customerId: 'cust_vip',
    items: [{ name: 'Server Rack', price: 5000 }],
    totalAmount: 5000
  });

  bus.execute({
    type: 'PayOrderCommand',
    orderId: 'ord_404',
    paymentReference: 'PAY_BANK_1',
    paidAmount: 5000
  });

  const summary = projections.getOrderSummary('ord_404');
  assert.strictEqual(summary.status, 'PAID');
  assert.strictEqual(summary.totalAmount, 5000);

  const kpis = projections.getKPIs();
  assert.strictEqual(kpis.totalOrders, 1);
  assert.strictEqual(kpis.totalRevenue, 5000);
  assert.strictEqual(projections.getCustomerSpend('cust_vip'), 5000);

  console.log('✓ Test 4 Passed: CQRS Projection Engine Materialized Views Verified');
}

// Test 5: Transactional Outbox Delivery
{
  let publishedToBroker = [];
  const outbox = new OutboxProcessor(async (evt) => {
    publishedToBroker.push(evt.type);
    return true;
  });

  outbox.enqueue({ type: 'ORDER_CREATED', aggregateId: 'ord_505' });
  outbox.enqueue({ type: 'ORDER_PAID', aggregateId: 'ord_505' });

  assert.strictEqual(outbox.getMetrics().queueLength, 2);
  outbox.processBatch(10).then(() => {
    assert.strictEqual(publishedToBroker.length, 2);
    assert.strictEqual(outbox.getMetrics().publishedCount, 2);
    console.log('✓ Test 5 Passed: Transactional Outbox Reliable Delivery Verified');
    console.log('ALL TESTS PASSED WITH 100% SUCCESS!');
  });
}
