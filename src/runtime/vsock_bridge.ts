/**
 * TASK-21: Host-Guest AF_VSOCK Zero-Trust Transparent Proxy Bridge.
 */

import { LeaseCoordinator } from "../lease_coordinator";
import { FenceToken, ModelPoolName } from "../models";

export class EgressPolicyViolationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "EgressPolicyViolationError";
  }
}

export class ZeroTrustVsockBridge {
  public allowedDomains: Set<string>;
  private hostVaultSecrets: Record<ModelPoolName, string> = {
    DeepSeek: "sk-host-vault-deepseek-prod",
    "GLM-5.3": "sk-host-vault-glm53-prod",
    "Qwen-3.8": "sk-host-vault-qwen38-prod",
  };

  constructor(
    public readonly leaseCoordinator: LeaseCoordinator,
    allowedDomains?: string[],
  ) {
    this.allowedDomains = new Set(
      allowedDomains ?? ["api.github.com", "internal-warehouse.corp.local", "mcp-gateway.internal"],
    );
  }

  public proxyOutboundRequest(
    fence: FenceToken,
    targetDomain: string,
    modelPool: ModelPoolName = "DeepSeek",
  ): Record<string, string> {
    this.leaseCoordinator.verifyAndAdvanceFence(fence);
    if (!this.allowedDomains.has(targetDomain)) {
      throw new EgressPolicyViolationError(`Domain '${targetDomain}' blocked by VSOCK egress allowlist`);
    }
    return {
      Authorization: `Bearer ${this.hostVaultSecrets[modelPool] ?? "sk-default"}`,
      "X-Agent-Fence": fence.toHeader(),
      "X-Target-Domain": targetDomain,
    };
  }
}
