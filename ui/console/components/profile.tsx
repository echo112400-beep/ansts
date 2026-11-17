"use client";
import { useEffect, useState } from "react";
import Image from "next/image";
import {
  ExternalLink,
  Github,
  LogOut,
  RefreshCw,
  ShieldCheck,
} from "lucide-react";
import type { User } from "../lib/console";
export type GithubProfile = {
  id: number;
  login: string;
  name: string | null;
  avatarUrl: string | null;
  url: string;
  bio: string | null;
  company: string | null;
  location: string | null;
  joinedAt: string | null;
};
export const roleName = (role: string) =>
  (
    ({
      SRE_ADMIN: "系统管理员",
      OPERATOR: "任务操作员",
      VIEWER: "只读成员",
    }) as Record<string, string>
  )[role] ?? role;
export function Avatar({
  profile,
  name,
  large = false,
}: {
  profile: GithubProfile | null;
  name: string;
  large?: boolean;
}) {
  const [broken, setBroken] = useState(false);
  useEffect(() => setBroken(false), [profile?.avatarUrl]);
  return (
    <span className={`avatar user-avatar ${large ? "large" : ""}`}>
      {profile?.avatarUrl && !broken ? (
        <Image
          src={profile.avatarUrl}
          width={large ? 68 : 29}
          height={large ? 68 : 29}
          alt=""
          referrerPolicy="no-referrer"
          onError={() => setBroken(true)}
        />
      ) : (
        name.slice(0, 1).toUpperCase()
      )}
    </span>
  );
}
export function Profile({
  user,
  profile,
  loading,
  error,
  expiresAt,
  retry,
  logout,
}: {
  user: User;
  profile: GithubProfile | null;
  loading: boolean;
  error: string;
  expiresAt: number;
  retry: () => void;
  logout: () => void;
}) {
  const github = user.sub.startsWith("github:");
  const name =
    profile?.name || profile?.login || (github ? "GitHub 用户" : user.sub);
  return (
    <div className="user-profile">
      <div className="profile-hero">
        <Avatar profile={profile} name={name} large />
        <div>
          <h3>{name}</h3>
          <p>
            {profile
              ? `@${profile.login}`
              : github
                ? "GitHub 账号"
                : "开发凭据登录"}
          </p>
        </div>
      </div>
      {loading && (
        <p role="status" className="subtle">
          正在从 GitHub 获取资料…
        </p>
      )}
      {error && (
        <div className="banner error" role="alert">
          暂时无法获取 GitHub 资料。
          <button onClick={retry}>
            <RefreshCw size={13} />
            重试
          </button>
          <a href="/v1/auth/github">重新连接 GitHub</a>
        </div>
      )}
      {profile?.bio && <p className="profile-bio">{profile.bio}</p>}
      {profile && (
        <>
          <dl className="detail-grid profile-facts">
            <div>
              <dt>GitHub 用户名</dt>
              <dd>{profile.login}</dd>
            </div>
            <div>
              <dt>加入 GitHub</dt>
              <dd>
                {profile.joinedAt
                  ? new Date(profile.joinedAt).toLocaleDateString("zh-CN")
                  : "未公开"}
              </dd>
            </div>
            <div>
              <dt>组织 / 公司</dt>
              <dd>{profile.company || "未公开"}</dd>
            </div>
            <div>
              <dt>所在地</dt>
              <dd>{profile.location || "未公开"}</dd>
            </div>
          </dl>
          <a
            className="secondary github-profile-link"
            href={profile.url}
            target="_blank"
            rel="noopener noreferrer"
          >
            <Github size={16} />
            查看 GitHub 主页
            <ExternalLink size={14} />
          </a>
        </>
      )}
      <section className="access-profile">
        <h3>
          <ShieldCheck size={16} />
          工作区权限
        </h3>
        <dl className="detail-grid">
          <div>
            <dt>应用角色</dt>
            <dd>{roleName(user.role)}</dd>
          </div>
          <div>
            <dt>租户范围</dt>
            <dd>
              {user.scopeTenant === "all" ? "全部租户" : user.scopeTenant}
            </dd>
          </div>
          <div>
            <dt>操作权限</dt>
            <dd>{user.canWrite ? "可查看并提交任务" : "仅查看"}</dd>
          </div>
          <div>
            <dt>会话有效至</dt>
            <dd>
              {expiresAt ? new Date(expiresAt).toLocaleString("zh-CN") : "—"}
            </dd>
          </div>
          <div className="identity-row">
            <dt>身份标识</dt>
            <dd className="mono">{user.sub}</dd>
          </div>
        </dl>
        <p>角色与租户权限由工作区管理员分配，GitHub 资料更新不会改变权限。</p>
      </section>
      <button className="secondary profile-logout" onClick={logout}>
        <LogOut size={15} />
        退出当前账号
      </button>
    </div>
  );
}
