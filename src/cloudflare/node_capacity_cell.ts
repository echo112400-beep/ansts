import { DurableObjectState, CloudflareCelldEnv } from "./env";
import { initializeCellProtocol } from "../persistence/schema";
import { NodeCapacity, NodeCapacityError, nodeCapacityMigration } from "../persistence/node-capacity";

/** Durable authority for one physical execution node's incarnation and resource reservations. */
export class NodeCapacityCell {
  public readonly capacity: NodeCapacity;
  public readonly nodeId: string;

  constructor(
    public readonly ctx: DurableObjectState,
    public readonly env: CloudflareCelldEnv,
  ) {
    this.nodeId = ctx.id.name ?? ctx.id.toString();
    initializeCellProtocol(ctx.storage, { type: "node-capacity", key: this.nodeId }, [nodeCapacityMigration]);
    this.capacity = new NodeCapacity(ctx.storage, this.nodeId);
  }

  public async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    try {
      if (request.method === "POST" && url.pathname === "/nodes/register") {
        const body = await request.json() as any;
        if (body.node_id !== undefined && body.node_id !== this.nodeId) {
          throw new NodeCapacityError("NODE_ID_MISMATCH", "Node registration does not match this Cell identity.");
        }
        const node = await this.capacity.register({
          incarnation: String(body.incarnation ?? ""),
          capacity: body.capacity,
          reboot_evidence_id: body.reboot_evidence_id,
        });
        return json({ node_id: node.node_id, incarnation: node.incarnation, capacity: node.capacity }, 200);
      }

      if (request.method === "POST" && url.pathname === "/heartbeat") {
        const body = await request.json() as any;
        const node = await this.capacity.heartbeat(String(body.incarnation ?? ""), body.heartbeat_at ?? Date.now());
        return json({ node_id: node.node_id, incarnation: node.incarnation, heartbeat_at: node.updated_at });
      }

      if (request.method === "POST" && url.pathname === "/reservations") {
        const body = await request.json() as any;
        const reservation = await this.capacity.reserve({
          dispatch_id: String(body.dispatch_id ?? ""),
          task_id: String(body.task_id ?? ""),
          tenant_id: String(body.tenant_id ?? ""),
          kind: body.kind,
          node_incarnation: String(body.node_incarnation ?? ""),
          resources: body.resources,
        });
        return json(reservation, 201);
      }

      if (request.method === "GET" && url.pathname === "/state") {
        const state = this.capacity.getState();
        return state ? json(state) : json({ error: "NODE_NOT_REGISTERED" }, 404);
      }

      if (request.method === "GET" && url.pathname === "/allocations") {
        return json({ allocations: this.capacity.getReservations(true) });
      }

      const reservationMatch = url.pathname.match(/^\/reservations\/([^/]+)(?:\/(authorize|started|suspect|release))?$/);
      if (reservationMatch) {
        const dispatchId = decodeURIComponent(reservationMatch[1]);
        const action = reservationMatch[2];
        if (!action && request.method === "GET") {
          const reservation = this.capacity.getReservation(dispatchId);
          return reservation ? json(reservation) : json({ error: "NODE_RESERVATION_NOT_FOUND" }, 404);
        }
        if (request.method !== "POST" || !action) return json({ error: "MethodNotAllowed" }, 405);
        const body = await request.json() as any;
        if (action === "authorize") {
          return json(await this.capacity.authorize({
            dispatch_id: dispatchId,
            reservation_token: String(body.reservation_token ?? ""),
            authorization_token: String(body.authorization_token ?? ""),
            business_epoch: Number(body.business_epoch),
            node_incarnation: String(body.node_incarnation ?? ""),
          }));
        }
        if (action === "started") {
          return json(await this.capacity.markStarted({
            dispatch_id: dispatchId,
            authorization_token: String(body.authorization_token ?? ""),
            business_epoch: Number(body.business_epoch),
            start_receipt: String(body.start_receipt ?? ""),
          }));
        }
        if (action === "suspect") return json(await this.capacity.markSuspect(dispatchId, String(body.reason ?? "")));
        return json(await this.capacity.release({
          dispatch_id: dispatchId,
          node_incarnation: String(body.node_incarnation ?? ""),
          evidence_type: body.evidence_type,
          evidence_id: String(body.evidence_id ?? ""),
        }));
      }

      return json({ error: "NotFound", path: url.pathname }, 404);
    } catch (error) {
      if (error instanceof NodeCapacityError) return json({ error: error.code, message: error.message }, error.status);
      throw error;
    }
  }
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}
