/**
 * Self-contained ambient declarations for Node.js built-in modules (`node:*`) and globals
 * so `tsc -p tsconfig.json` compiles cleanly even in offline/air-gapped environments before `npm install`.
 */
declare global {
  interface Buffer extends Uint8Array {
    toString(encoding?: string, start?: number, end?: number): string;
    writeUInt32BE(value: number, offset?: number): number;
    readUInt32BE(offset?: number): number;
    readUInt16BE(offset?: number): number;
    subarray(begin?: number, end?: number): Buffer;
  }
  var Buffer: {
    from(data: string | Uint8Array | ArrayBuffer | number[], encoding?: string): Buffer;
    alloc(size: number, fill?: number | string, encoding?: string): Buffer;
    concat(list: readonly Uint8Array[], totalLength?: number): Buffer;
    byteLength(string: string | Uint8Array, encoding?: string): number;
  };
  var process: {
    cwd(): string;
    exit(code?: number): never;
    execPath: string;
    argv: string[];
    env: Record<string, string | undefined>;
    memoryUsage(): { rss: number; heapTotal: number; heapUsed: number; external: number };
  };
}

declare module "node:crypto" {
  export interface Hash {
    update(data: string | Uint8Array, inputEncoding?: string): Hash;
    digest(): Buffer;
    digest(encoding: "hex" | "base64"): string;
  }
  export function createHash(algorithm: string): Hash;
  export function createHmac(algorithm: string, key: string | Uint8Array): Hash;
  export function randomBytes(size: number): Buffer;
}

declare module "node:fs" {
  export interface Dirent {
    name: string;
    isDirectory(): boolean;
    isFile(): boolean;
  }
  export interface Stats {
    isFile(): boolean;
    isDirectory(): boolean;
    mode: number;
    size: number;
  }
  export function existsSync(path: string): boolean;
  export function mkdirSync(path: string, options?: { recursive?: boolean }): string | undefined;
  export function mkdtempSync(prefix: string): string;
  export function writeFileSync(path: string, data: string | Uint8Array, options?: string | { encoding?: string; mode?: number }): void;
  export function readFileSync(path: string, encoding: "utf-8" | "utf8"): string;
  export function readFileSync(path: string): Buffer;
  export function readdirSync(path: string): string[];
  export function readdirSync(path: string, options: { withFileTypes: true }): Dirent[];
  export function statSync(path: string): Stats;
  export function rmSync(path: string, options?: { recursive?: boolean; force?: boolean }): void;
}

declare module "node:path" {
  export const sep: string;
  export const delimiter: string;
  export function join(...paths: string[]): string;
  export function resolve(...paths: string[]): string;
  export function dirname(p: string): string;
  export function basename(p: string, ext?: string): string;
  export function relative(from: string, to: string): string;
}

declare module "node:os" {
  export function tmpdir(): string;
}

declare module "node:stream" {
  export class PassThrough {
    write(chunk: string | Uint8Array): boolean;
    read(): string | Buffer | null;
    end(): void;
  }
}

declare module "node:http" {
  export interface IncomingMessage {
    method?: string;
    url?: string;
    headers: Record<string, string | string[] | undefined>;
    on(event: "data", listener: (chunk: Buffer) => void): this;
    on(event: "end", listener: () => void): this;
  }
  export interface ServerResponse {
    writeHead(statusCode: number, headers?: Record<string, string>): this;
    end(data?: string | Uint8Array): this;
  }
  export interface Server {
    listen(port: number, hostname: string, backlog?: () => void): this;
    address(): { port: number; address: string } | string | null;
    close(callback?: (err?: Error) => void): this;
    on(event: string, listener: (err: Error) => void): this;
  }
  export function createServer(requestListener?: (req: IncomingMessage, res: ServerResponse) => void): Server;
}

declare module "node:net" {
  export interface Socket {
    on(event: "data", listener: (chunk: Buffer) => void): this;
    on(event: "error", listener: (err: Error) => void): this;
    write(data: Uint8Array | string): boolean;
    end(): this;
  }
  export interface Server {
    listen(path: string, listeningListener?: () => void): this;
    close(callback?: (err?: Error) => void): this;
    on(event: string, listener: (err: Error) => void): this;
  }
  export function createServer(connectionListener?: (socket: Socket) => void): Server;
  export function createConnection(path: string, connectListener?: () => void): Socket;
}

declare module "node:child_process" {
  export function execSync(command: string, options?: { cwd?: string; stdio?: string; timeout?: number }): Buffer;
}

declare module "node:vm" {
  export type Context = Record<string, unknown>;
  export function createContext(sandbox?: Record<string, unknown>): Context;
  export class Script {
    constructor(code: string, options?: { filename?: string });
    runInContext(contextifiedObject: Context, options?: { timeout?: number }): unknown;
  }
}

declare module "node:zlib" {
  export function deflateRawSync(buf: Uint8Array, options?: { level?: number }): Buffer;
  export function inflateRawSync(buf: Uint8Array): Buffer;
}

declare module "node:test" {
  export function describe(name: string, fn: () => void | Promise<void>): void;
  export function it(name: string, fn: () => void | Promise<void>): void;
}

declare module "node:assert/strict" {
  export function ok(value: unknown, message?: string): asserts value;
  export function equal<T>(actual: T, expected: T, message?: string): void;
  export function deepEqual<T>(actual: T, expected: T, message?: string): void;
  export function throws(block: () => unknown, error?: RegExp | ((err: unknown) => boolean), message?: string): void;
  export function rejects(block: () => Promise<unknown>, error?: RegExp | unknown, message?: string): Promise<void>;
}

export {};
