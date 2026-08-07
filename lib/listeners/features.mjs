/**
 * Feature listeners — side effects for feature domain events.
 */

import { on, publishEvent } from "../events/index.mjs";

on("feature.status_changed", async ({ id, status, name }) => {
  await publishEvent("event.feature.status_changed", { id, status, name });
});
