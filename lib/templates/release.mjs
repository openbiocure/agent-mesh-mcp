/**
 * Release template — returns structured data for rendering.
 */

import { releaseStatusIcon } from "./shared.mjs";

export function renderRelease(release) {
  const prList = release.prs || [];
  return {
    icon: releaseStatusIcon(release.status),
    name: release.name,
    id: release.id.slice(0, 8),
    fullId: release.id,
    status: release.status,
    prs: prList,
  };
}

export function renderReleaseList(releases) {
  return {
    title: `Releases (${releases.length})`,
    icon: "🚀",
    items: releases.map(renderRelease),
  };
}
