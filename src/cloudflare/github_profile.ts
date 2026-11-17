/** Public GitHub presentation data; never used as an authorization source. */
import type { ConsoleIdentityProfile } from "../dashboard/console_identity_profile.js";
export type GithubProfile = ConsoleIdentityProfile;
const profiles = new Map<number, { value: GithubProfile; expires: number }>();
const text = (value: unknown, limit = 256): string | null => {
  if (typeof value !== "string") return null;
  const clean = value.replace(/[\u0000-\u001f\u007f]/g, "").trim();
  let result = "",
    bytes = 0;
  for (const char of clean) {
    bytes += Buffer.byteLength(char, "utf8");
    if (bytes > limit) break;
    result += char;
  }
  return result || null;
};
export function rememberGithubProfile(
  raw: Record<string, unknown>,
): GithubProfile {
  if (
    !Number.isSafeInteger(raw.id) ||
    Number(raw.id) < 1 ||
    typeof raw.login !== "string" ||
    !/^[a-zA-Z0-9][a-zA-Z0-9-]{0,38}$/.test(raw.login)
  )
    throw new Error("INVALID_GITHUB_PROFILE");
  let avatarUrl: string | null = null;
  try {
    const avatar = new URL(String(raw.avatar_url));
    if (
      avatar.protocol === "https:" &&
      avatar.hostname === "avatars.githubusercontent.com" &&
      !avatar.username &&
      !avatar.password &&
      !avatar.port &&
      avatar.pathname.startsWith("/u/") &&
      avatar.toString().length <= 512
    )
      avatarUrl = avatar.toString();
  } catch {}
  const value: GithubProfile = {
    id: Number(raw.id),
    login: raw.login,
    name: text(raw.name, 128),
    avatarUrl,
    url: `https://github.com/${raw.login}`,
    bio: text(raw.bio, 512),
    company: text(raw.company),
    location: text(raw.location),
    joinedAt:
      typeof raw.created_at === "string" &&
      Number.isFinite(Date.parse(raw.created_at))
        ? new Date(raw.created_at).toISOString()
        : null,
  };
  if (profiles.size >= 256) profiles.delete(profiles.keys().next().value!);
  profiles.set(value.id, { value, expires: Date.now() + 300000 });
  return value;
}
export async function githubProfileForSubject(
  subject: string,
  snapshot?: GithubProfile,
): Promise<GithubProfile | null> {
  const match = /^github:([1-9][0-9]{0,15})$/.exec(subject);
  if (!match || !Number.isSafeInteger(Number(match[1]))) return null;
  const id = Number(match[1]);
  if (snapshot?.id === id)
    return rememberGithubProfile({
      ...snapshot,
      avatar_url: snapshot.avatarUrl,
      created_at: snapshot.joinedAt,
    });
  const cached = profiles.get(id);
  if (cached && cached.expires > Date.now()) return cached.value;
  const response = await fetch(`https://api.github.com/user/${id}`, {
    headers: {
      accept: "application/vnd.github+json",
      "user-agent": "ANSTS",
      "x-github-api-version": "2022-11-28",
    },
    redirect: "error",
    signal: AbortSignal.timeout(5000),
  });
  if (!response.ok || !response.body)
    throw new Error("GITHUB_PROFILE_UNAVAILABLE");
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      total += chunk.value.length;
      if (total > 65536) {
        await reader.cancel();
        throw new Error("GITHUB_PROFILE_TOO_LARGE");
      }
      chunks.push(chunk.value);
    }
  } finally {
    reader.releaseLock();
  }
  const raw = JSON.parse(Buffer.concat(chunks).toString("utf8")) as Record<
    string,
    unknown
  >;
  if (raw.id !== id) throw new Error("GITHUB_PROFILE_ID_MISMATCH");
  return rememberGithubProfile(raw);
}
