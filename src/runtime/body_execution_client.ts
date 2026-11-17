/** Native long execution transport, without fetch/Undici's five-minute response-header deadline. */
import * as http from "node:http";
export function requestBodyExecution(endpoint: string, payload: unknown, headers: Record<string, string>, signal: AbortSignal): Promise<Record<string, unknown>> {
  const url = new URL(endpoint);
  if (url.protocol !== "http:" || url.hostname !== "127.0.0.1") throw new Error("LOCAL_EXECUTION_ENDPOINT_REQUIRED");
  return new Promise((resolve, reject) => {
    const body = JSON.stringify(payload);
    const request = http.request(url, { method: "POST", headers: { ...headers, "content-type": "application/json", "content-length": Buffer.byteLength(body) }, signal }, response => {
      const chunks: Buffer[] = []; let size = 0;
      response.on("error", reject);
      response.on("data", chunk => { size += chunk.length; if (size > 65536) response.destroy(new Error("BODY_RESULT_TOO_LARGE")); else chunks.push(chunk); });
      response.on("end", () => {
        if ((response.statusCode ?? 500) >= 300) { reject(new Error(`BODY_EXECUTION_STATUS_${response.statusCode}`)); return; }
        try {
          const value = JSON.parse(Buffer.concat(chunks).toString("utf8"));
          if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("INVALID_BODY_RESULT");
          resolve(value);
        } catch (error) { reject(error); }
      });
    });
    const connecting = setTimeout(() => request.destroy(new Error("BODY_CONNECT_TIMEOUT")), 5000);
    request.on("socket", socket => { if (socket.connecting) socket.once("connect", () => clearTimeout(connecting)); else clearTimeout(connecting); });
    request.on("close", () => clearTimeout(connecting)); request.on("error", reject); request.end(body);
  });
}
