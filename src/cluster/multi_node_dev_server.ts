/**
 * @fileoverview v2.5 Multi-Node Role-Aware Cluster Server (`brain` | `body` | `gateway`) for `deploy/dev-cluster`.
 */
import { AnstsLocalAndComposeServer, AnstsServiceRuntimeOptions } from "../server_main.js";

export class MultiNodeDevServer extends AnstsLocalAndComposeServer {
  public readonly nodeId: string;
  public readonly nodeRole: "brain" | "body" | "gateway";

  constructor(options?: AnstsServiceRuntimeOptions & { nodeId?: string; nodeRole?: "brain" | "body" | "gateway" }) {
    super(options);
    this.nodeId = options?.nodeId ?? process.env.ANSTS_NODE_ID ?? "ansts-dev-node-01";
    this.nodeRole =
      (options?.nodeRole ?? (process.env.ANSTS_NODE_ROLE as "brain" | "body" | "gateway")) || "gateway";
  }
}
