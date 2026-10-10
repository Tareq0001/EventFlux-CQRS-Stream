/**
 * Transactional Outbox Processor
 * Guarantees reliable at-least-once message delivery to message brokers (Kafka/RabbitMQ simulator).
 */
class OutboxProcessor {
  constructor(brokerPublisher) {
    this.outboxQueue = [];
    this.deadLetterQueue = [];
    this.brokerPublisher = brokerPublisher || (async (evt) => true);
    this.isProcessing = false;
    this.publishedCount = 0;
  }

  enqueue(event) {
    this.outboxQueue.push({
      id: 'outbox_' + Math.random().toString(36).substring(2, 8),
      event,
      status: 'PENDING',
      attempts: 0,
      maxAttempts: 3,
      createdAt: Date.now()
    });
  }

  async processBatch(batchSize = 10) {
    if (this.isProcessing) return;
    this.isProcessing = true;

    const pending = this.outboxQueue.filter(m => m.status === 'PENDING').slice(0, batchSize);

    for (const msg of pending) {
      msg.attempts++;
      try {
        await this.brokerPublisher(msg.event);
        msg.status = 'PUBLISHED';
        msg.publishedAt = Date.now();
        this.publishedCount++;
      } catch (err) {
        if (msg.attempts >= msg.maxAttempts) {
          msg.status = 'FAILED';
          msg.error = err.message;
          this.deadLetterQueue.push({ ...msg });
        }
      }
    }

    this.isProcessing = false;
  }

  getMetrics() {
    return {
      queueLength: this.outboxQueue.filter(m => m.status === 'PENDING').length,
      publishedCount: this.publishedCount,
      dlqCount: this.deadLetterQueue.length
    };
  }
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { OutboxProcessor };
}
