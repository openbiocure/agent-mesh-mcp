/**
 * Release listeners — side effects for release domain events.
 */

import { on, publishEvent } from "../events/index.mjs";
import { notifySubscribers } from "../notifications/index.mjs";
import { triggerDeployIfReady } from "./deploy-queue.mjs";

on("release.status_changed", async ({ id, name, status, incident_id }) => {
  await notifySubscribers(id, name || "unknown", status);

  if (["ready", "deploying"].includes(status)) {
    await publishEvent(`event.release.${status}`, { id, name, incident_id });
  }

  if (status === "ready") {
    await triggerDeployIfReady();
  }
});

on("release.closed", async ({ id, name, status, incident_id, summary }) => {
  await notifySubscribers(id, name || "unknown", status, summary);
  await publishEvent(`event.release.${status}`, { id, name, incident_id, summary });
  await triggerDeployIfReady();
});
