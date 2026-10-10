/**
 * Append-Only Event Store
 * Implements Optimistic Concurrency Control (OCC) and snapshotting.
 */
class EventStore {
  constructor() {
    this.streams = new Map(); // aggregateId -> Array of events
    this.globalLog = [];      // Array of all events in ingestion order (offset indexed)
    this.snapshots = new Map(); // aggregateId -> { version, state }
    this.listeners = new Set();
  }

  appendEvents(aggregateId, events, expectedVersion) {
    const currentStream = this.streams.get(aggregateId) || [];
    const currentVersion = currentStream.length;

    // Optimistic Concurrency Check
    if (expectedVersion !== undefined && expectedVersion !== null && expectedVersion !== currentVersion) {
      throw new Error(
        `ConcurrencyConflict: Aggregate ${aggregateId} expected version ${expectedVersion} but found version ${currentVersion}`
      );
    }

    const stampedEvents = events.map((evt, idx) => {
      const offset = this.globalLog.length + idx;
      return {
        ...evt,
        offset,
        storedAt: Date.now()
      };
    });

    // Append to stream
    this.streams.set(aggregateId, [...currentStream, ...stampedEvents]);

    // Append to global log
    stampedEvents.forEach(evt => {
      this.globalLog.push(evt);
      this.notifyListeners(evt);
    });

    return stampedEvents;
  }

  getEventsForAggregate(aggregateId, fromVersion = 0) {
    const stream = this.streams.get(aggregateId) || [];
    return stream.slice(fromVersion);
  }

  getGlobalEvents(fromOffset = 0, limit = 100) {
    return this.globalLog.slice(fromOffset, fromOffset + limit);
  }

  takeSnapshot(aggregateId, version, state) {
    this.snapshots.set(aggregateId, {
      version,
      state: JSON.parse(JSON.stringify(state)),
      takenAt: Date.now()
    });
  }

  getSnapshot(aggregateId) {
    return this.snapshots.get(aggregateId) || null;
  }

  subscribe(listener) {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  notifyListeners(event) {
    this.listeners.forEach(fn => {
      try { fn(event); } catch (e) { console.error('Event subscriber error:', e); }
    });
  }
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { EventStore };
}
