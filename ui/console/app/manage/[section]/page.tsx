import Console from "../../../components/console";
import { ADMIN_IDS } from "../../../lib/admin";
import { notFound } from "next/navigation";
export default async function ManagementPage({
  params,
}: {
  params: Promise<{ section: string }>;
}) {
  const { section } = await params;
  if (!ADMIN_IDS.includes(section)) notFound();
  return <Console initialView={section} />;
}
