/**
 * Tool registry — imports all domain tools and registers them on the MCP server.
 */

import { registerAgentTools } from "./agents/index.mjs";
import { registerSchedulingTools } from "./scheduling/index.mjs";
import { registerPromptTools } from "./prompts/index.mjs";
import { registerIncidentTools } from "./incidents/index.mjs";
import { registerReleaseTools } from "./releases/index.mjs";
import { registerApprovalTools } from "./approvals/index.mjs";
import { registerMeshBugTools } from "./mesh-bugs/index.mjs";
import { registerSettingsTools } from "./settings/index.mjs";
import { registerConversationTools } from "./conversations/index.mjs";

export function registerTools(server) {
  registerAgentTools(server);
  registerSchedulingTools(server);
  registerPromptTools(server);
  registerIncidentTools(server);
  registerReleaseTools(server);
  registerApprovalTools(server);
  registerMeshBugTools(server);
  registerSettingsTools(server);
  registerConversationTools(server);
}
