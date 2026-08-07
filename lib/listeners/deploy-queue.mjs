/**
 * Deploy queue — dispatches one release at a time.
 * P1/P2 hotfixes skip the queue and deploy immediately.
 */

import prisma from "../db/index.mjs";
import { sendNotification } from "../notifications/index.mjs";
import { publishMessage } from "../events/index.mjs";

export async function triggerDeployIfReady() {
  try {
    const deploying = await prisma.release.findFirst({
      where: { status: "deploying" },
      select: { id: true, name: true, startedAt: true },
    });

    const next = await prisma.release.findFirst({
      where: { status: "ready" },
      orderBy: { createdAt: "asc" },
      select: { id: true, name: true, incidentId: true },
    });

    if (!next) return;

    let isHotfix = false;
    if (next.incidentId) {
      const incident = await prisma.incident.findUnique({
        where: { id: next.incidentId },
        select: { severity: true },
      });
      isHotfix = incident && ["p1", "p2"].includes(incident.severity);
    }

    if (deploying && !isHotfix) {
      const elapsed = deploying.startedAt
        ? (Date.now() - new Date(deploying.startedAt).getTime()) / 60000
        : 999;
      if (elapsed < 30) return;

      await prisma.release.update({
        where: { id: deploying.id },
        data: { status: "ready" },
      });
      await sendNotification({ type: "warning", title: `Release ${deploying.name} stuck ${Math.round(elapsed)}min — reclaimed`, workerName: "deploy-queue" });
    }

    if (deploying && isHotfix) {
      await sendNotification({ type: "incident", title: `HOTFIX ${next.name} — jumping deploy queue`, workerName: "deploy-queue" });
    }

    await publishMessage("ask.prod-ops", {
      from: "deploy-queue",
      message: `Deploy release ${next.id.slice(0, 8)} "${next.name}". Run get_release("${next.id.slice(0, 8)}"), follow the steps, close_release when done.`,
    });

    await sendNotification({ type: "deploy", title: `Deploying: ${next.name} (${next.id.slice(0, 8)})${isHotfix ? " [HOTFIX]" : ""}`, workerName: "deploy-queue" });
  } catch (err) {
    console.error("Deploy queue error:", err.message);
  }
}
