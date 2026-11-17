/** Display-only identity snapshot, signed with the application session. */
export interface ConsoleIdentityProfile {
  id: number;
  login: string;
  name: string | null;
  avatarUrl: string | null;
  url: string;
  bio: string | null;
  company: string | null;
  location: string | null;
  joinedAt: string | null;
}
