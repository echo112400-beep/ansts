/**
 * @fileoverview TASK-27 & M27 (TASK-126): CNCF CloudEvents 1.0 Bus with Dynamic Cryptographic
 * W3C `traceparent` (`00-<32hex>-<16hex>-01`) Generation & Custom Metadata.
 */

import * as crypto from "node:crypto";

export class CloudEventsBus {
  public events: Array<Record<string, any>> = [];

  public emit(
    eventType: string,
    taskId: string,
    tenantId: string,
    data: Record<string, any>,
    customMetadata: Record<string, string> = {},
  ): Record<string, any> {
    const traceId = crypto.randomBytes(16).toString("hex");
    const spanId = crypto.randomBytes(8).toString("hex");
    const dynamicTraceparent = customMetadata.traceparent ?? `00-${traceId}-${spanId}-01`;

    const envelope = {
      specversion: "1.0",
      id: `evt_${crypto.randomBytes(8).toString("hex")}`,
      source: `/ansts/us-west-2/${taskId}`,
      type: eventType,
      subject: `task/${taskId}`,
      time: new Date().toISOString(),
      datacontenttype: "application/json",
      traceparent: dynamicTraceparent,
      data: {
        task_id: taskId,
        tenant_id: tenantId,
        custom_metadata: customMetadata,
        ...data,
      },
    };
    this.events.push(envelope);
    return envelope;
  }
}
