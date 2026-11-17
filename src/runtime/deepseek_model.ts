/** Model credentials stay in the Daemon and are never passed to a sandbox. */
import {createHash} from 'node:crypto';
import type {ProviderUsage} from '../control/usage_event.js';
export type ModelUsageResponse=Omit<ProviderUsage,'call'|'provider'|'requestedModel'>;
export interface AgentMessage { role: "system" | "user" | "assistant"; content: string; }
export interface AgentModel { readonly usageSource?:{provider:string;model:string};next(messages: AgentMessage[], maxTokens: number, signal: AbortSignal, onUsage?:(usage:ModelUsageResponse)=>void): Promise<{ action: Record<string, unknown>; tokens: number }>; }
export class DeepSeekModel implements AgentModel {
  readonly usageSource:{provider:string;model:string};
  constructor(private readonly key: string, private readonly model = "deepseek-flash",private readonly transport:typeof fetch=fetch) { if (!key.trim() || key.length > 1024 || /\s/.test(key) || !/^[a-zA-Z0-9._-]{1,128}$/.test(model)) throw new Error("INVALID_MODEL_CONFIGURATION");this.usageSource={provider:'deepseek',model}; }
  async next(messages: AgentMessage[], maxTokens: number, signal: AbortSignal,onUsage?:(usage:ModelUsageResponse)=>void): Promise<{ action: Record<string, unknown>; tokens: number }> {
    let response: Response;
    try { response = await this.transport("https://api.deepseek.com/chat/completions", { method: "POST", headers: { authorization: `Bearer ${this.key}`, "content-type": "application/json" }, body: JSON.stringify({ model: this.model, messages, max_tokens: maxTokens, temperature: 0, thinking: { type: "disabled" }, response_format: { type: "json_object" }, stream: false }), signal: AbortSignal.any([signal, AbortSignal.timeout(60000)]), redirect: "error" }); }
    catch (error) { if (signal.aborted) throw error; throw new Error("MODEL_TRANSPORT_FAILED"); }
    if ([429, 502, 503, 504].includes(response.status)) { await response.body?.cancel(); throw new Error("MODEL_TEMPORARILY_UNAVAILABLE"); }
    if (!response.ok || !response.body) throw new Error("MODEL_REQUEST_FAILED");
    const reader = response.body.getReader(); const chunks: Uint8Array[] = []; let size = 0;
    try { for (;;) { const part = await reader.read(); if (part.done) break; size += part.value.length; if (size > 65536) { await reader.cancel(); throw new Error("MODEL_RESPONSE_LIMIT"); } chunks.push(part.value); } }
    catch (error) { if (signal.aborted || (error instanceof Error && error.message === "MODEL_RESPONSE_LIMIT")) throw error; throw new Error("MODEL_TRANSPORT_FAILED"); }
    finally { reader.releaseLock(); }
    let result;
    try { result = JSON.parse(Buffer.concat(chunks).toString("utf8")); } catch { throw new Error("MODEL_RESPONSE_JSON_INVALID"); }
    const token=(value:unknown)=>typeof value==='number'&&Number.isSafeInteger(value)&&value>=0?String(value):null;
    const label=(value:unknown)=>typeof value==='string'&&value.trim()&&value.length<=256&&!/[\u0000-\u001f]/.test(value)?value:null;
    const inputTokens=token(result?.usage?.prompt_tokens),outputTokens=token(result?.usage?.completion_tokens),totalTokens=token(result?.usage?.total_tokens);
    let cacheHitTokens=token(result?.usage?.prompt_cache_hit_tokens),cacheMissTokens=token(result?.usage?.prompt_cache_miss_tokens);
    const invalidCache=(result?.usage?.prompt_cache_hit_tokens!==undefined&&cacheHitTokens===null)||(result?.usage?.prompt_cache_miss_tokens!==undefined&&cacheMissTokens===null)||(inputTokens!==null&&((cacheHitTokens!==null&&BigInt(cacheHitTokens)>BigInt(inputTokens))||(cacheMissTokens!==null&&BigInt(cacheMissTokens)>BigInt(inputTokens))||(cacheHitTokens!==null&&cacheMissTokens!==null&&BigInt(cacheHitTokens)+BigInt(cacheMissTokens)!==BigInt(inputTokens))));
    if(invalidCache){cacheHitTokens=null;cacheMissTokens=null;}
    const reportedModel=label(result?.model),responseId=label(result?.id);
    // Record response quantities before interpreting actions: invalid model output may still cost tokens.
    onUsage?.({reportedModel,responseId,inputTokens,outputTokens,totalTokens,cacheHitTokens,cacheMissTokens,evidenceDigest:createHash('sha256').update(Buffer.concat(chunks)).digest('hex'),source:'PROVIDER_RESPONSE',completeness:inputTokens!==null&&outputTokens!==null&&totalTokens!==null&&BigInt(inputTokens)+BigInt(outputTokens)===BigInt(totalTokens)&&!invalidCache&&reportedModel!==null&&responseId!==null?'COMPLETE':'MISSING'});
    const content = result.choices?.[0]?.message?.content;
    if (typeof content !== "string" || !Number.isSafeInteger(result.usage?.total_tokens) || result.usage.total_tokens < 1 || result.choices[0].finish_reason !== "stop") throw new Error("INVALID_MODEL_RESPONSE");
    let action;
    try { action = JSON.parse(content); } catch { throw new Error("MODEL_ACTION_JSON_INVALID"); }
    if (!action || typeof action !== "object" || Array.isArray(action)) throw new Error("INVALID_MODEL_ACTION");
    return { action, tokens: result.usage.total_tokens };
  }
}
