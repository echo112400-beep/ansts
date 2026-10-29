/**
 * TASK-27: CNCF CloudEvents 1.0 Bus with OpenTelemetry Trace Context & Custom Metadata.
 */

import * as crypto from "crypto";

export class CloudEventsBus {
  public events: Array<Record<string, any>> = [];

  public emit(
    eventType: string,
    taskId: string,
    tenantId: string,
    data: Record<string, any>,
    customMetadata: Record<string, string> = {},
  ): Record<string, any> {
    const envelope = {
      specversion: "1.0",
      id: `evt_${crypto.randomBytes(8).toString("hex")}`,
      source: `/ansts/us-west-2/${taskId}`,
      type: eventType,
      subject: `task/${taskId}`,
      datacontenttype: "application/json",
      traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
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
