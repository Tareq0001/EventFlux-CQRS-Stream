/**
 * Aggregate Root Base Class & Order Aggregate
 * Enforces business invariants and reconstitutes state through event playback.
 */
class AggregateRoot {
  constructor(id) {
    this.id = id;
    this.version = 0;
    this.uncommittedEvents = [];
  }

  applyEvent(event, isNew = false) {
    const handler = 'on' + event.type.split('_').map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join('');
    if (typeof this[handler] === 'function') {
      this[handler](event);
    }
    this.version = event.metadata.version || (this.version + 1);
    if (isNew) {
      this.uncommittedEvents.push(event);
    }
  }

  loadFromHistory(events) {
    events.forEach(evt => this.applyEvent(evt, false));
  }

  clearUncommittedEvents() {
    const events = [...this.uncommittedEvents];
    this.uncommittedEvents = [];
    return events;
  }
}

class OrderAggregate extends AggregateRoot {
  constructor(id) {
    super(id);
    this.customerId = null;
    this.items = [];
    this.totalAmount = 0;
    this.status = 'INITIAL';
    this.trackingNumber = null;
  }

  // Commands
  createOrder(customerId, items, totalAmount) {
    if (this.status !== 'INITIAL') {
      throw new Error(`Order ${this.id} already exists in status ${this.status}`);
    }
    if (!items || items.length === 0) {
      throw new Error('Order items cannot be empty');
    }
    if (totalAmount <= 0) {
      throw new Error('Total amount must be greater than zero');
    }

    const { OrderCreatedEvent } = typeof require !== 'undefined' ? require('./event') : window;
    const evt = new OrderCreatedEvent(this.id, customerId, items, totalAmount, { version: this.version + 1 });
    this.applyEvent(evt, true);
  }

  payOrder(paymentReference, paidAmount) {
    if (this.status !== 'PENDING') {
      throw new Error(`Cannot pay order in status ${this.status}`);
    }
    if (paidAmount < this.totalAmount) {
      throw new Error(`Insufficient payment: expected ${this.totalAmount}, got ${paidAmount}`);
    }

    const { OrderPaidEvent } = typeof require !== 'undefined' ? require('./event') : window;
    const evt = new OrderPaidEvent(this.id, paymentReference, paidAmount, { version: this.version + 1 });
    this.applyEvent(evt, true);
  }

  shipOrder(trackingNumber, carrier) {
    if (this.status !== 'PAID') {
      throw new Error(`Cannot ship order that is not paid. Current status: ${this.status}`);
    }

    const { OrderShippedEvent } = typeof require !== 'undefined' ? require('./event') : window;
    const evt = new OrderShippedEvent(this.id, trackingNumber, carrier, { version: this.version + 1 });
    this.applyEvent(evt, true);
  }

  cancelOrder(reason) {
    if (this.status === 'SHIPPED') {
      throw new Error('Cannot cancel order that has already been shipped');
    }
    if (this.status === 'CANCELLED') {
      throw new Error('Order is already cancelled');
    }

    const { OrderCancelledEvent } = typeof require !== 'undefined' ? require('./event') : window;
    const evt = new OrderCancelledEvent(this.id, reason, { version: this.version + 1 });
    this.applyEvent(evt, true);
  }

  // Event Handlers (State Reconstitution)
  onOrderCreated(event) {
    this.customerId = event.payload.customerId;
    this.items = [...event.payload.items];
    this.totalAmount = event.payload.totalAmount;
    this.status = 'PENDING';
  }

  onOrderPaid(event) {
    this.status = 'PAID';
    this.paymentReference = event.payload.paymentReference;
  }

  onOrderShipped(event) {
    this.status = 'SHIPPED';
    this.trackingNumber = event.payload.trackingNumber;
    this.carrier = event.payload.carrier;
  }

  onOrderCancelled(event) {
    this.status = 'CANCELLED';
    this.cancelReason = event.payload.reason;
  }
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { AggregateRoot, OrderAggregate };
}
