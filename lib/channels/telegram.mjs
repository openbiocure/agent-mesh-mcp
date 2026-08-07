/**
 * Telegram channel renderer — takes rendered template data and formats for Telegram (plain text).
 */

export function formatIncidentList(rendered) {
  return `${rendered.icon} ${rendered.title}\n\n` +
    rendered.items.map(i =>
      `${i.icon} [${i.severity}] ${i.title}\n${i.id} · ${i.status}`
    ).join("\n\n");
}

export function formatIncidentStatusList(rendered, lines) {
  return `${rendered.icon} ${rendered.title}\n\n${lines.join("\n\n")}`;
}

export function formatIncidentStatusLine(item) {
  return `${item.icon} [${item.severity}] ${item.title}\n   ${item.id} · ${item.status}\n   ${item.agentStatus}`;
}

export function formatReleaseList(rendered) {
  return `${rendered.icon} ${rendered.title}\n\n` +
    rendered.items.map(r => {
      const prs = r.prs.length > 3
        ? `\nPRs: ${r.prs.slice(0, 3).join(", ")} +${r.prs.length - 3} more`
        : r.prs.length
          ? `\nPRs: ${r.prs.join(", ")}`
          : "";
      return `${r.icon} ${r.name}\n${r.id} · ${r.status}${prs}`;
    }).join("\n\n");
}

export function formatAgentList(rendered) {
  return `${rendered.icon} ${rendered.title}\n\n` +
    rendered.items.map(a =>
      `● ${a.name}\n   ${a.host} · ${a.topics.join(", ")}`
    ).join("\n\n");
}

export function formatActivityTail(agentName, activities) {
  if (activities.length === 0) {
    return `No recent activity from ${agentName}`;
  }
  return `🔍 ${agentName} — last ${activities.length} actions:\n\n` +
    activities.map(a => `${a.timeAgo}: ${a.text}`).join("\n\n");
}

export function formatNotification({ icon, title, workerName, message }) {
  let text = `${icon} ${title}`;
  if (workerName) text += `\nAgent: ${workerName}`;
  if (message) text += `\n\n${message}`;
  text += "\n\nReply to respond to the agent.";
  return text;
}
