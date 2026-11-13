/**
 * @fileoverview Gateway celld entrypoint. Serves the gateway HTTP API and AnstsGatewayCell.
 * The scheduler is a different celld process reached at ANSTS_SCHEDULER_URL.
 */
import { UnifiedGatewayEnv, handleUnifiedGatewayRequest } from "./gateway_cell.js";

export { AnstsGatewayCell } from "./gateway_cell.js";

const JSON_HEADERS = { "content-type": "application/json" };

export default {
  async fetch(request: Request, env: UnifiedGatewayEnv): Promise<Response> {
    const handled = await handleUnifiedGatewayRequest(request, env);
    if (handled) return handled;
    return new Response(JSON.stringify({ error: "NOT_FOUND", controlPlane: "celld-gateway" }), {
      status: 404,
      headers: JSON_HEADERS,
    });
  },
};
