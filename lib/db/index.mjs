import { PrismaClient, Prisma } from "@prisma/client";

const prisma = new PrismaClient();
export default prisma;

/**
 * Resolve a short ID prefix to a full UUID.
 * Uses raw SQL because Prisma doesn't support casting + LIKE on UUIDs.
 */
export async function resolveId(table, shortId) {
  if (shortId.length >= 32) return shortId;
  const rows = await prisma.$queryRaw`SELECT id::text as id FROM ${Prisma.raw(table)} WHERE id::text LIKE ${shortId + "%"} LIMIT 2`;
  if (rows.length === 1) return rows[0].id;
  if (rows.length === 0) throw new Error(`No ${table} found matching '${shortId}'`);
  throw new Error(`Ambiguous: ${rows.length} ${table} match '${shortId}'`);
}
