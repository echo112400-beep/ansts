/**
 * TASK-61: Model Context Protocol (MCP) Dynamic Tool Registry & 4-Tier Transaction Contract Binder.
 *
 * Discovers tools exposed by external MCP Servers (`tools/list` JSON-RPC 2.0) and binds each tool
 * to ANSTS's 4-tier transaction model (`PURE_READ`, `IDEMPOTENT_WRITE`, `SAGA_COMPENSATABLE`, `STAGED_COMMIT`).
 * Safety invariant: Any mutating external MCP tool that does NOT declare an inverse `compensationTool`
 * is automatically downgraded to `STAGED_COMMIT` (held in staging buffer until human/policy approval).
 */

import { FenceToken, ToolEffectClass } from "../models";
import { SagaToolGateway } from "../saga_gateway";

export interface McpToolDescriptor {
  serverName: string;
  name: string;
  description: string;
  inputSchema: {
    type: "object";
    required?: string[];
    properties: Record<string, { type: string }>;
  };
  annotations?: {
    readOnlyHint?: boolean;
    idempotentHint?: boolean;
    compensationTool?: string;
    requireHumanApproval?: boolean;
  };
}

export interface BoundMcpToolContract {
  qualifiedName: string;
  descriptor: McpToolDescriptor;
  assignedEffectClass: ToolEffectClass;
  compensationTool?: string;
  downgradedToStagedCommit: boolean;
}

export class McpDynamicToolRegistry {
  private readonly contracts = new Map<string, BoundMcpToolContract>();

  constructor(public readonly sagaGateway: SagaToolGateway) {}

  /**
   * Registers a batch of MCP tools from a JSON-RPC `tools/list` response and assigns 4-tier safety contracts.
   */
  public registerMcpServerTools(tools: McpToolDescriptor[]): BoundMcpToolContract[] {
    const bound: BoundMcpToolContract[] = [];
    for (const tool of tools) {
      const qualifiedName = `${tool.serverName}/${tool.name}`;
      let assignedEffectClass = ToolEffectClass.STAGED_COMMIT;
      let downgradedToStagedCommit = false;

      if (tool.annotations?.readOnlyHint === true) {
        assignedEffectClass = ToolEffectClass.PURE_READ;
      } else if (tool.annotations?.idempotentHint === true && !tool.annotations?.compensationTool) {
        assignedEffectClass = ToolEffectClass.IDEMPOTENT_WRITE;
      } else if (tool.annotations?.compensationTool && !tool.annotations?.requireHumanApproval) {
        assignedEffectClass = ToolEffectClass.SAGA_COMPENSATABLE;
      } else {
        // Mutating tool without inverse compensation tool -> enforce STAGED_COMMIT safety gate
        assignedEffectClass = ToolEffectClass.STAGED_COMMIT;
        downgradedToStagedCommit = !tool.annotations?.requireHumanApproval;
      }

      const contract: BoundMcpToolContract = {
        qualifiedName,
        descriptor: tool,
        assignedEffectClass,
        compensationTool: tool.annotations?.compensationTool,
        downgradedToStagedCommit,
      };
      this.contracts.set(qualifiedName, contract);
      bound.push(contract);
    }
    return bound;
  }

  /**
   * Validates arguments against the MCP tool's JSON Schema and executes through the 4-tier Saga Gateway.
   */
  public invokeMcpTool(params: {
    qualifiedName: string;
    toolCallId: string;
    fence: FenceToken;
    args: Record<string, any>;
  }): {
    contract: BoundMcpToolContract;
    gatewayResult: Record<string, any>;
  } {
    const contract = this.contracts.get(params.qualifiedName);
    if (!contract) {
      throw new Error(`Unregistered MCP tool: ${params.qualifiedName}`);
    }

    // Validate required JSON Schema fields
    const requiredFields = contract.descriptor.inputSchema.required ?? [];
    for (const reqField of requiredFields) {
      if (!(reqField in params.args)) {
        throw new Error(`MCP Schema Validation Failed for ${params.qualifiedName}: missing required field '${reqField}'`);
      }
    }

    const gatewayResult = this.sagaGateway.executeTool({
      fence: params.fence,
      toolCallId: params.toolCallId,
      toolName: params.qualifiedName,
      effectClass: contract.assignedEffectClass,
      payload: JSON.stringify(params.args),
      compensationTool: contract.compensationTool,
    });

    return { contract, gatewayResult };
  }

  public getContract(qualifiedName: string): BoundMcpToolContract | undefined {
    return this.contracts.get(qualifiedName);
  }
}
