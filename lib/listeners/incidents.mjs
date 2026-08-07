/**
 * Incident listeners — side effects for incident domain events.
 */

import { on, publishEvent } from "../events/index.mjs";
import { sendNotification } from "../notifications/index.mjs";

on("incident.created", async ({ id, repo, title, severity, gh_issue, assigned_agent, created_by }) => {
  await publishEvent("event.incident.created", {
    id, repo, title, severity, gh_issue, assigned_agent, created_by,
  });
});

on("incident.created", async ({ repo, title, severity, gh_issue, summary, created_by }) => {
  if (!["p1", "p2"].includes(severity)) return;
  const ghRef = gh_issue ? ` (${repo}#${gh_issue})` : "";
  await sendNotification({
    type: "incident",
    title: `NEW ${severity.toUpperCase()}: ${title}${ghRef}`,
    message: `Repo: ${repo}${summary ? "\n" + summary : ""}`,
    workerName: created_by?.toLowerCase().replace(/\s+/g, "-") || "unknown",
    workerSid: process.env.WORKER_SID || null,
  });
});

on("incident.status_changed", async ({ id, status, severity, assigned_agent, gh_pr }) => {
  await publishEvent("event.incident.status_changed", {
    id, status, severity, assigned_agent, gh_pr,
  });
});

on("incident.resolved", async ({ id, gh_pr }) => {
  await publishEvent("event.incident.resolved", { id, gh_pr });
});
