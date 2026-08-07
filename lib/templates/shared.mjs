/**
 * Shared icons and formatting helpers used across templates.
 */

export function severityIcon(severity) {
  if (severity === "p1") return "🔴";
  if (severity === "p2") return "🟠";
  return "🟡";
}

export function releaseStatusIcon(status) {
  if (status === "deploying") return "🔄";
  if (status === "ready") return "✅";
  if (status === "failed") return "❌";
  return "🔨";
}

export const notificationIcons = {
  incident: "🔴",
  deploy:   "🚀",
  info:     "📋",
  warning:  "⚠️",
  success:  "✅",
  error:    "❌",
};

export function notificationIcon(type) {
  return notificationIcons[type] || notificationIcons.info;
}

/**
 * Format a timestamp into a human-readable "X ago" string.
 * Accepts a Date, ISO string, or numeric timestamp.
 */
export function timeAgo(timestamp) {
  const ago = Math.round((Date.now() - new Date(timestamp).getTime()) / 1000);
  if (ago < 60) return `${ago}s ago`;
  if (ago < 3600) return `${Math.round(ago / 60)}m ago`;
  return `${Math.round(ago / 3600)}h ago`;
}

/**
 * Format a timestamp into a short "Xs/Xm/Xh" string (no "ago" suffix).
 */
export function timeAgoShort(timestamp) {
  const ago = Math.round((Date.now() - new Date(timestamp).getTime()) / 1000);
  if (ago < 60) return `${ago}s`;
  if (ago < 3600) return `${Math.round(ago / 60)}m`;
  return `${Math.round(ago / 3600)}h`;
}
