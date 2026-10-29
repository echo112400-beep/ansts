/**
 * TASK-04: SSE Event Stream Gateway with Last-Event-ID Resume and Client Credit Window Flow Control.
 */

export interface SseEventFrame {
  eventId: number;
  eventType: string;
  taskId: string;
  payload: string;
}

export class ClientStreamSubscription {
  public availableCredits: number;
  public pendingDeltaBuffer: string[] = [];

  constructor(
    public readonly clientId: string,
    public readonly maxCreditWindow: number = 100,
  ) {
    this.availableCredits = maxCreditWindow;
  }

  public get creditRatio(): number {
    return this.availableCredits / Math.max(1, this.maxCreditWindow);
  }
}

export class SseEventStreamGateway {
  private eventRing: SseEventFrame[] = [];
  private nextId: number = 1;
  private clients: Map<string, ClientStreamSubscription> = new Map();

  public registerClient(clientId: string, maxCredits: number = 100): ClientStreamSubscription {
    const sub = new ClientStreamSubscription(clientId, maxCredits);
    this.clients.set(clientId, sub);
    return sub;
  }

  public ackCredits(clientId: string, ackedCount: number): number {
    const sub = this.clients.get(clientId);
    if (!sub) return 1.0;
    sub.availableCredits = Math.min(sub.maxCreditWindow, sub.availableCredits + ackedCount);
    return sub.creditRatio;
  }

  public publishEvent(clientId: string, eventType: string, taskId: string, payload: string): SseEventFrame[] {
    let sub = this.clients.get(clientId);
    if (!sub) {
      sub = this.registerClient(clientId);
    }
    const emitted: SseEventFrame[] = [];

    if (sub.creditRatio < 0.25 && eventType === "agent.harness.thinking.delta") {
      sub.pendingDeltaBuffer.push(payload);
      if (
        payload.endsWith(".") ||
        payload.endsWith("!") ||
        payload.endsWith("?") ||
        payload.endsWith("。") ||
        sub.pendingDeltaBuffer.length >= 4
      ) {
        const merged = sub.pendingDeltaBuffer.join("");
        sub.pendingDeltaBuffer = [];
        const frame: SseEventFrame = {
          eventId: this.nextId++,
          eventType: "agent.harness.thinking.coalesced",
          taskId,
          payload: merged,
        };
        this.eventRing.push(frame);
        sub.availableCredits = Math.max(0, sub.availableCredits - 1);
        emitted.push(frame);
      }
      return emitted;
    }

    const frame: SseEventFrame = {
      eventId: this.nextId++,
      eventType,
      taskId,
      payload,
    };
    this.eventRing.push(frame);
    sub.availableCredits = Math.max(0, sub.availableCredits - 1);
    emitted.push(frame);
    return emitted;
  }

  public resumeFromLastEventId(lastEventId: number): SseEventFrame[] {
    return this.eventRing.filter((f) => f.eventId > lastEventId);
  }
}
