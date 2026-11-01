import {
  PersistenceCellBindings,
  PersistenceCellNamespace,
  PersistenceCellStub,
  PersistenceCellType,
} from "./contracts";

/** These class names are durable namespace identities; renaming one creates a new, empty namespace. */
export const PERSISTENCE_CELL_CLASS_NAMES: Record<PersistenceCellType, string> = {
  "tenant-ledger": "TenantLedgerCell",
  task: "AgentTaskHarnessCell",
  "scheduler-pool": "AnstsSchedulerCell",
  "node-capacity": "NodeCapacityCell",
  dag: "DagCell",
  "provider-quota": "ProviderQuotaCell",
  "routing-config": "RoutingConfigCell",
  "tenant-directory": "TenantDirectoryCell",
};

export interface PersistenceWorkerBindings extends PersistenceCellBindings {
  SCHEDULER_CELL: PersistenceCellNamespace;
  AGENT_HARNESS_CELL: PersistenceCellNamespace;
}

export function persistenceCellIdentity(type: PersistenceCellType, key: string): { type: PersistenceCellType; key: string } {
  if (typeof key !== "string" || key.trim().length === 0 || /[\u0000-\u001f\u007f]/.test(key)) {
    throw new TypeError("Cell routing key must be a non-empty string without control characters.");
  }
  if (new TextEncoder().encode(key).byteLength > 1024) {
    throw new TypeError("Cell routing key must be at most 1024 UTF-8 bytes so Celld retains its logical name.");
  }
  return { type, key };
}

/** Resolve a logical owner to its currently configured stable namespace binding. */
export function persistenceCellNamespace(
  bindings: PersistenceWorkerBindings,
  type: PersistenceCellType,
): PersistenceCellNamespace {
  const namespace = (() => {
    switch (type) {
      case "task":
        return bindings.TASK_CELL ?? bindings.AGENT_HARNESS_CELL;
      case "scheduler-pool":
        return bindings.SCHEDULER_POOL_CELL ?? bindings.SCHEDULER_CELL;
      case "tenant-ledger":
        return bindings.TENANT_LEDGER_CELL;
      case "node-capacity":
        return bindings.NODE_CAPACITY_CELL;
      case "dag":
        return bindings.DAG_CELL;
      case "provider-quota":
        return bindings.PROVIDER_QUOTA_CELL;
      case "routing-config":
        return bindings.ROUTING_CONFIG_CELL;
      case "tenant-directory":
        return bindings.TENANT_DIRECTORY_CELL;
    }
  })();

  if (!namespace) {
    throw new Error(`Missing Durable Object binding for ${type} (${PERSISTENCE_CELL_CLASS_NAMES[type]}).`);
  }
  return namespace;
}

/** Use only the logical entity key as the durable object name; the class namespace provides type isolation. */
export function persistenceCellStub(
  bindings: PersistenceWorkerBindings,
  type: PersistenceCellType,
  key: string,
): PersistenceCellStub {
  const identity = persistenceCellIdentity(type, key);
  const namespace = persistenceCellNamespace(bindings, identity.type);
  return namespace.get(namespace.idFromName(identity.key));
}
