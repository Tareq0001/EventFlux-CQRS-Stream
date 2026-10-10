/**
 * Domain Event Definitions
 * Immutable record of something that happened in the domain.
 */
class DomainEvent {
  constructor(aggregateId, type, payload, metadata = {}) {
    this.eventId = 'evt_' + Math.random().toString(36).substring(2, 9) + Date.now().toString(36);
    this.aggregateId = aggregateId;
    this.type = type;
    this.payload = Object.freeze({ ...payload });
    this.metadata = Object.freeze({
      timestamp: Date.now(),
      correlationId: metadata.correlationId || 'corr_' + Math.random().toString(36).substring(2, 7),
      causationId: metadata.causationId || null,
      version: metadata.version || 1,
      ...metadata
    });
    Object.freeze(this);
  }
}

class OrderCreatedEvent extends DomainEvent {
  constructor(aggregateId, customerId, items, totalAmount, metadata) {
    super(aggregateId, 'ORDER_CREATED', { customerId, items, totalAmount, status: 'PENDING' }, metadata);
  }
}

class OrderPaidEvent extends DomainEvent {
  constructor(aggregateId, paymentReference, paidAmount, metadata) {
    super(aggregateId, 'ORDER_PAID', { paymentReference, paidAmount, status: 'PAID' }, metadata);
  }
}

class OrderShippedEvent extends DomainEvent {
  constructor(aggregateId, trackingNumber, carrier, metadata) {
    super(aggregateId, 'ORDER_SHIPPED', { trackingNumber, carrier, status: 'SHIPPED' }, metadata);
  }
}

class OrderCancelledEvent extends DomainEvent {
  constructor(aggregateId, reason, metadata) {
    super(aggregateId, 'ORDER_CANCELLED', { reason, status: 'CANCELLED' }, metadata);
  }
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    DomainEvent,
    OrderCreatedEvent,
    OrderPaidEvent,
    OrderShippedEvent,
    OrderCancelledEvent
  };
}
