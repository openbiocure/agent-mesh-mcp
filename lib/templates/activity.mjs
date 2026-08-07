/**
 * Activity template — returns structured data for rendering.
 */

import { timeAgo } from "./shared.mjs";

export function renderActivity(activity) {
  const ago = timeAgo(activity.timestamp);
  return {
    text: (activity.content || "").slice(0, 120),
    timeAgo: ago,
  };
}

export function renderActivityShort(activity) {
  const ago = timeAgo(activity.timestamp);
  return {
    text: (activity.content || "").slice(0, 100),
    timeAgo: ago,
  };
}
