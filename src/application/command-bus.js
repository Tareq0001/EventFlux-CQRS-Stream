/**
 * Command Bus & Command Handlers
 * Mediates commands, reconstitutes aggregates, executes domain logic, and commits events.
 */
class CommandBus {
  constructor(eventStore, outboxProcessor) {
    this.eventStore = eventStore;
    this.outboxProcessor = outboxProcessor;
    this.handlers = new Map();
    this.registerDefaultHandlers();
  }

  registerHandler(commandType, handlerFn) {
    this.handlers.set(commandType, handlerFn);
  }

  async execute(command) {
    const handler = this.handlers.get(command.type);
    if (!handler) {
      throw new Error(`No command handler registered for ${command.type}`);
    }
    return await handler(command, this.eventStore, this.outboxProcessor);
  }

  registerDefaultHandlers() {
    const { OrderAggregate } = typeof require !== 'undefined' ? require('../domain/aggregate') : window;

    // Handle CreateOrderCommand
    this.registerHandler('CreateOrderCommand', async (cmd, store, outbox) => {
      const aggregate = new OrderAggregate(cmd.orderId);
      const history = store.getEventsForAggregate(cmd.orderId);
      aggregate.loadFromHistory(history);

      aggregate.createOrder(cmd.customerId, cmd.items, cmd.totalAmount);
      const events = aggregate.clearUncommittedEvents();
      
      const storedEvents = store.appendEvents(cmd.orderId, events, aggregate.version - events.length);
      if (outbox) {
        storedEvents.forEach(evt => outbox.enqueue(evt));
      }
      return { success: true, orderId: cmd.orderId, events: storedEvents };
    });

    // Handle PayOrderCommand
    this.registerHandler('PayOrderCommand', async (cmd, store, outbox) => {
      const aggregate = new OrderAggregate(cmd.orderId);
      const history = store.getEventsForAggregate(cmd.orderId);
      if (history.length === 0) {
        throw new Error(`Order ${cmd.orderId} does not exist`);
      }
      aggregate.loadFromHistory(history);

      aggregate.payOrder(cmd.paymentReference, cmd.paidAmount);
      const events = aggregate.clearUncommittedEvents();

      const storedEvents = store.appendEvents(cmd.orderId, events, aggregate.version - events.length);
      if (outbox) {
        storedEvents.forEach(evt => outbox.enqueue(evt));
      }
      return { success: true, orderId: cmd.orderId, events: storedEvents };
    });

    // Handle ShipOrderCommand
    this.registerHandler('ShipOrderCommand', async (cmd, store, outbox) => {
      const aggregate = new OrderAggregate(cmd.orderId);
      const history = store.getEventsForAggregate(cmd.orderId);
      aggregate.loadFromHistory(history);

      aggregate.shipOrder(cmd.trackingNumber, cmd.carrier);
      const events = aggregate.clearUncommittedEvents();

      const storedEvents = store.appendEvents(cmd.orderId, events, aggregate.version - events.length);
      if (outbox) {
        storedEvents.forEach(evt => outbox.enqueue(evt));
      }
      return { success: true, orderId: cmd.orderId, events: storedEvents };
    });

    // Handle CancelOrderCommand
    this.registerHandler('CancelOrderCommand', async (cmd, store, outbox) => {
      const aggregate = new OrderAggregate(cmd.orderId);
      const history = store.getEventsForAggregate(cmd.orderId);
      aggregate.loadFromHistory(history);

      aggregate.cancelOrder(cmd.reason);
      const events = aggregate.clearUncommittedEvents();

      const storedEvents = store.appendEvents(cmd.orderId, events, aggregate.version - events.length);
      if (outbox) {
        storedEvents.forEach(evt => outbox.enqueue(evt));
      }
      return { success: true, orderId: cmd.orderId, events: storedEvents };
    });
  }
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { CommandBus };
}
