/**
 * Template dispatcher — re-exports all template renderers.
 */

export { renderIncident, renderIncidentList, renderIncidentStatus, renderIncidentStatusList } from "./incident.mjs";
export { renderRelease, renderReleaseList } from "./release.mjs";
export { renderAgent, renderAgentList } from "./agent.mjs";
export { renderActivity, renderActivityShort } from "./activity.mjs";
export { severityIcon, releaseStatusIcon, notificationIcon, timeAgo, timeAgoShort } from "./shared.mjs";
