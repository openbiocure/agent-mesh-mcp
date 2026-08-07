/**
 * Agent template — returns structured data for rendering.
 */

export function renderAgent(worker) {
  return {
    name: worker.name,
    host: worker.host,
    topics: worker.topics || [],
  };
}

export function renderAgentList(workers) {
  return {
    title: `Agents (${workers.length})`,
    icon: "🤖",
    items: workers.map(renderAgent),
  };
}
