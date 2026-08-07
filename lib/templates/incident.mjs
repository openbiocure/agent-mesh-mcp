/**
 * Incident template — returns structured data for rendering.
 */

import { severityIcon } from "./shared.mjs";

export function renderIncident(incident) {
  return {
    icon: severityIcon(incident.severity),
    severity: incident.severity.toUpperCase(),
    title: incident.title,
    id: incident.id.slice(0, 8),
    fullId: incident.id,
    status: incident.status,
    assignedAgent: incident.assignedAgent || null,
  };
}

export function renderIncidentList(incidents) {
  return {
    title: `Incidents (${incidents.length})`,
    icon: "📋",
    items: incidents.map(renderIncident),
  };
}

export function renderIncidentStatus(incident, agentStatus) {
  return {
    ...renderIncident(incident),
    agentStatus,
  };
}

export function renderIncidentStatusList(incidents) {
  return {
    title: `Active Incidents (${incidents.length})`,
    icon: "📋",
    items: incidents,
  };
}
