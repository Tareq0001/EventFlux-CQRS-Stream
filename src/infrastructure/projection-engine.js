/**
 * CQRS Projection Engine (Read Model Projections)
 * Converts event streams into high-throughput query-optimized materialized views.
 */
class ProjectionEngine {
  constructor(eventStore) {
    this.eventStore = eventStore;
    this.orderSummaries = new Map(); // orderId -> summary object
    this.customerRevenue = new Map(); // customerId -> total spend
    this.kpis = {
      totalOrders: 0,
      totalRevenue: 0,
      paidOrders: 0,
      shippedOrders: 0,
      cancelledOrders: 0
    };
    this.processedOffset = 0;

    if (this.eventStore) {
      this.eventStore.subscribe(event => this.project(event));
    }
  }

  project(event) {
    switch (event.type) {
      case 'ORDER_CREATED': {
        this.orderSummaries.set(event.aggregateId, {
          orderId: event.aggregateId,
          customerId: event.payload.customerId,
          itemCount: event.payload.items.length,
          totalAmount: event.payload.totalAmount,
          status: 'PENDING',
          createdAt: event.metadata.timestamp,
          updatedAt: event.metadata.timestamp
        });
        this.kpis.totalOrders++;
        break;
      }
      case 'ORDER_PAID': {
        const order = this.orderSummaries.get(event.aggregateId);
        if (order) {
          order.status = 'PAID';
          order.updatedAt = event.metadata.timestamp;
          this.kpis.paidOrders++;
          this.kpis.totalRevenue += event.payload.paidAmount;

          const currentRev = this.customerRevenue.get(order.customerId) || 0;
          this.customerRevenue.set(order.customerId, currentRev + event.payload.paidAmount);
        }
        break;
      }
      case 'ORDER_SHIPPED': {
        const order = this.orderSummaries.get(event.aggregateId);
        if (order) {
          order.status = 'SHIPPED';
          order.trackingNumber = event.payload.trackingNumber;
          order.carrier = event.payload.carrier;
          order.updatedAt = event.metadata.timestamp;
          this.kpis.shippedOrders++;
        }
        break;
      }
      case 'ORDER_CANCELLED': {
        const order = this.orderSummaries.get(event.aggregateId);
        if (order) {
          order.status = 'CANCELLED';
          order.cancelReason = event.payload.reason;
          order.updatedAt = event.metadata.timestamp;
          this.kpis.cancelledOrders++;
        }
        break;
      }
    }
    this.processedOffset = event.offset !== undefined ? event.offset + 1 : this.processedOffset + 1;
  }

  rebuildAll(events) {
    this.orderSummaries.clear();
    this.customerRevenue.clear();
    this.kpis = { totalOrders: 0, totalRevenue: 0, paidOrders: 0, shippedOrders: 0, cancelledOrders: 0 };
    this.processedOffset = 0;
    events.forEach(evt => this.project(evt));
  }

  getOrderSummary(orderId) {
    return this.orderSummaries.get(orderId) || null;
  }

  getAllOrders() {
    return Array.from(this.orderSummaries.values());
  }

  getCustomerSpend(customerId) {
    return this.customerRevenue.get(customerId) || 0;
  }

  getKPIs() {
    return { ...this.kpis };
  }
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { ProjectionEngine };
}
