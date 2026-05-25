/**
 * @license MIT
 * @copyright 2026
 * Drift - High-performance event streaming engine.
 */

export interface Event {
  id: string;
  topic: string;
  payload: any;
  timestamp: number;
}

export type Handler = (event: Event) => Promise<void>;

/**
 * DriftStream manages event production and consumption.
 */
export class DriftStream {
  private handlers: Map<string, Handler[]> = new Map();

  /**
   * Subscribes a handler to a specific topic.
   * @param topic - The topic name to listen to.
   * @param handler - Callback function executed on new events.
   */
  public subscribe(topic: string, handler: Handler): void {
    const topicHandlers = this.handlers.get(topic) || [];
    this.handlers.set(topic, [...topicHandlers, handler]);
  }

  /**
   * Publishes an event to a topic.
   * @param topic - The target topic.
   * @param payload - Data to be streamed.
   */
  public async publish(topic: string, payload: any): Promise<void> {
    const event: Event = {
      id: Math.random().toString(36).substring(7),
      topic,
      payload,
      timestamp: Date.now(),
    };

    const topicHandlers = this.handlers.get(topic) || [];
    await Promise.all(topicHandlers.map(handler => handler(event)));
  }
}
