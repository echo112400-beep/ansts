/**
 * @fileoverview v2.5 Multi-Node Role-Aware Cluster Server (`brain` | `body` | `gateway`) for `deploy/dev-cluster`.
 */
import { AnstsLocalAndComposeServer, AnstsServiceRuntimeOptions } from "../server_main.js";

export class MultiNodeDevServer extends AnstsLocalAndComposeServer {
  constructor(options?: AnstsServiceRuntimeOptions) {
    super(options);
  }
}
