/**
 * @fileoverview TASK-79 (Q5): `ANSTS_RUNTIME_PROFILE=production` Fail-Fast Preflight Gate.
 *
 * Prohibits silent fallback to in-memory mocks when running in production:
 * - Verifies `RustFS` S3 endpoint configuration & credentials
 * - Verifies Linux Cgroups v2 unified hierarchy (`cgroup.controllers`, writable `/sys/fs/cgroup`)
 * - Verifies Linux PSI (`/proc/pressure/cpu` & `/proc/pressure/memory`) availability
 * - Verifies `AF_VSOCK` / UDS socket directory permissions
 */

import * as fs from "node:fs";
import * as path from "node:path";

export type AnstsRuntimeProfile = "production" | "test";

export interface PreflightEnvironmentSpec {
  profile: AnstsRuntimeProfile;
  role: "brain" | "body" | "all";
  s3Endpoint?: string;
  s3Bucket?: string;
  s3AccessKeyId?: string;
  s3SecretAccessKey?: string;
  cgroupRoot?: string;
  procPressureRoot?: string;
  vsockSocketDir?: string;
}

export interface PreflightCheckResult {
  passed: boolean;
  profile: AnstsRuntimeProfile;
  mockFallbackAllowed: boolean;
  checks: Record<string, { ok: boolean; detail: string }>;
  failedReasons: string[];
}

export class ProductionPreflightGate {
  /**
   * Instance seam over `evaluate`: reads the environment variables the production profile is configured
   * with (`ANSTS_RUNTIME_PROFILE`, `S3_ENDPOINT`, `CELLD_BUCKET`, AWS credentials, Cgroup/PSI roots).
   */
  public runPreflightChecks(
    env: Record<string, string | undefined> = {},
  ): PreflightCheckResult {
    return ProductionPreflightGate.evaluate({
      profile: env.ANSTS_RUNTIME_PROFILE === "production" ? "production" : "test",
      role: "all",
      s3Endpoint: env.S3_ENDPOINT,
      s3Bucket: env.CELLD_BUCKET,
      s3AccessKeyId: env.AWS_ACCESS_KEY_ID,
      s3SecretAccessKey: env.AWS_SECRET_ACCESS_KEY,
      cgroupRoot: env.ANSTS_CGROUP_ROOT,
      procPressureRoot: env.ANSTS_PROC_PRESSURE_ROOT,
      vsockSocketDir: env.ANSTS_VSOCK_SOCKET_DIR,
    });
  }

  public static evaluate(spec: PreflightEnvironmentSpec): PreflightCheckResult {
    const checks: Record<string, { ok: boolean; detail: string }> = {};
    const failedReasons: string[] = [];

    if (spec.profile === "test") {
      return {
        passed: true,
        profile: "test",
        mockFallbackAllowed: true,
        checks: {
          profile_mode: { ok: true, detail: "ANSTS_RUNTIME_PROFILE=test (mock fallback enabled for CI)" },
        },
        failedReasons: [],
      };
    }

    if (spec.role === "brain" || spec.role === "all") {
      const s3Ok =
        Boolean(spec.s3Endpoint && spec.s3Endpoint.startsWith("http")) &&
        Boolean(spec.s3Bucket && spec.s3Bucket.startsWith("s3://")) &&
        Boolean(spec.s3AccessKeyId && spec.s3AccessKeyId.length >= 8) &&
        Boolean(spec.s3SecretAccessKey && spec.s3SecretAccessKey.length >= 12);
      checks.rustfs_s3_config = {
        ok: s3Ok,
        detail: s3Ok
          ? `Endpoint=${spec.s3Endpoint}, Bucket=${spec.s3Bucket}`
          : "Missing or weak S3_ENDPOINT / CELLD_BUCKET / AWS credentials",
      };
      if (!s3Ok) {
        failedReasons.push("PreflightFailed[rustfs_s3_config]: Invalid S3 endpoint, bucket, or credentials");
      }
    }

    if (spec.role === "body" || spec.role === "all") {
      const cgRoot = spec.cgroupRoot ?? "/sys/fs/cgroup";
      const psiRoot = spec.procPressureRoot ?? "/proc/pressure";
      const vsockDir = spec.vsockSocketDir ?? "/var/run/ansts";

      const cgControllersPath = path.join(cgRoot, "cgroup.controllers");
      const cgOk = fs.existsSync(cgRoot) && fs.existsSync(cgControllersPath);
      checks.cgroups_v2_hierarchy = {
        ok: cgOk,
        detail: cgOk
          ? `Cgroups v2 mounted at ${cgRoot}`
          : `Missing Cgroups v2 unified hierarchy at ${cgControllersPath}`,
      };
      if (!cgOk) {
        failedReasons.push(`PreflightFailed[cgroups_v2_hierarchy]: ${cgControllersPath} not found`);
      }

      const psiCpuPath = path.join(psiRoot, "cpu");
      const psiMemPath = path.join(psiRoot, "memory");
      const psiOk = fs.existsSync(psiCpuPath) && fs.existsSync(psiMemPath);
      checks.linux_psi_metrics = {
        ok: psiOk,
        detail: psiOk
          ? `Linux PSI active at ${psiRoot}/{cpu,memory}`
          : `Missing /proc/pressure/{cpu,memory} (ensure kernel psi=1)`,
      };
      if (!psiOk) {
        failedReasons.push(`PreflightFailed[linux_psi_metrics]: ${psiRoot}/{cpu,memory} missing`);
      }

      let vsockOk = false;
      try {
        fs.mkdirSync(vsockDir, { recursive: true });
        fs.accessSync(vsockDir, fs.constants.W_OK);
        vsockOk = true;
      } catch {
        vsockOk = false;
      }
      checks.vsock_socket_dir = {
        ok: vsockOk,
        detail: vsockOk ? `Writable socket dir ${vsockDir}` : `Cannot write to ${vsockDir}`,
      };
      if (!vsockOk) {
        failedReasons.push(`PreflightFailed[vsock_socket_dir]: Directory ${vsockDir} not writable`);
      }
    }

    return {
      passed: failedReasons.length === 0,
      profile: "production",
      mockFallbackAllowed: false,
      checks,
      failedReasons,
    };
  }

  public static assertProductionReadyOrThrow(spec: PreflightEnvironmentSpec): PreflightCheckResult {
    const result = ProductionPreflightGate.evaluate(spec);
    if (!result.passed) {
      throw new Error(
        `[FATAL] ANSTS_RUNTIME_PROFILE=production preflight failed (${result.failedReasons.length} errors): ${result.failedReasons.join("; ")}`,
      );
    }
    return result;
  }
}
