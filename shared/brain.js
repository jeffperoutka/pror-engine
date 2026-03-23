/**
 * The Brain — unified AI coordinator for PROR Engine
 * Handles natural language routing, context awareness, and cross-agent intelligence
 */
const ai = require('./ai');
const db = require('./airtable');
const slack = require('./slack');

const SYSTEM_PROMPT = `You are PROR Engine, Jeff's unified AI operating system for managing link building and Reddit marketing services.

You have access to these agents:
1. LINK BUILDER — Discovers prospects, pulls emails, generates outreach copy, creates Brevo campaigns, manages link placements
2. REDDIT STRATEGIST — Generates Reddit comments/posts for clients with brand alignment
3. QA CHECKER — Reviews deliverables against quality standards
4. ORCHESTRATOR — Coordinates all agents, tracks client progress, generates reports

You are talking to Jeff (the business owner) or his team. Be direct, concise, and action-oriented.

When Jeff asks about client status, pull from the data you're given.
When Jeff gives feedback on tone, style, or process — acknowledge and note it applies going forward.
When Jeff asks you to do something, figure out which agent handles it and execute.

Current date: ${new Date().toISOString().split('T')[0]}`;

/**
 * Route a natural language message to the right agent/action
 */
async function route(message, context = {}) {
  const routingPrompt = `Given this message from Jeff, determine what action to take.

Message: "${message}"
Channel: ${context.channel || 'unknown'}
Thread: ${context.thread ? 'yes' : 'no'}

Respond with JSON:
{
  "agent": "link-builder" | "reddit" | "qa" | "orchestrator" | "general",
  "action": "brief description of what to do",
  "client": "client name if mentioned, or null",
  "needsData": ["clients", "links", "reddit", "finances"] // which data to fetch
}`;

  return ai.json(routingPrompt, SYSTEM_PROMPT);
}

/**
 * Handle a general conversation message (in #command-center or DM)
 */
async function handleMessage(message, context = {}) {
  // Route the message
  const routing = await route(message, context);

  // Fetch any needed data
  let dataContext = '';
  if (routing.needsData?.includes('clients')) {
    const clients = await db.getClients();
    dataContext += `\n\nActive Clients:\n${JSON.stringify(clients, null, 2)}`;
  }
  if (routing.client && routing.needsData?.includes('links')) {
    const links = await db.getLinks(routing.client);
    dataContext += `\n\nLinks for ${routing.client}:\n${JSON.stringify(links, null, 2)}`;
  }
  if (routing.client && routing.needsData?.includes('reddit')) {
    const reddit = await db.getReddit(routing.client);
    dataContext += `\n\nReddit for ${routing.client}:\n${JSON.stringify(reddit, null, 2)}`;
  }
  if (routing.needsData?.includes('finances')) {
    const month = new Date().toISOString().slice(0, 7);
    const finances = await db.getFinances({ month });
    dataContext += `\n\nFinances (${month}):\n${JSON.stringify(finances, null, 2)}`;
  }

  // Generate response
  const response = await ai.complete(
    `${message}\n\nContext:${dataContext}`,
    SYSTEM_PROMPT + `\n\nRouting decision: ${JSON.stringify(routing)}`,
    { maxTokens: 2048 }
  );

  return {
    response,
    routing,
  };
}

/**
 * Handle a slash command
 */
async function handleCommand(command, args, context = {}) {
  switch (command) {
    case '/links':
      return { agent: 'link-builder', args: parseArgs(args) };
    case '/reddit':
      return { agent: 'reddit', args: parseArgs(args) };
    case '/qa':
      return { agent: 'qa', args: parseArgs(args) };
    case '/sprint':
      return { agent: 'orchestrator', args: parseArgs(args) };
    case '/clients':
      return { agent: 'orchestrator', args: { action: 'list-clients', ...parseArgs(args) } };
    case '/status':
      return { agent: 'orchestrator', args: { action: 'status', ...parseArgs(args) } };
    case '/finances':
      return { agent: 'orchestrator', args: { action: 'finances', ...parseArgs(args) } };
    default:
      return { agent: 'general', args: { raw: args } };
  }
}

/**
 * Parse space-separated args into key-value pairs
 * Supports: /links branvas 20 dr40+
 */
function parseArgs(argsString) {
  if (!argsString) return {};
  const parts = argsString.trim().split(/\s+/);
  const result = { raw: argsString };

  // First arg is usually client name
  if (parts[0] && !parts[0].includes('=')) {
    result.client = parts[0];
  }

  // Look for count (number)
  for (const p of parts) {
    if (/^\d+$/.test(p)) result.count = parseInt(p);
    if (/^dr\d+/i.test(p)) result.minDR = parseInt(p.replace(/^dr/i, ''));
  }

  // Key=value pairs
  for (const p of parts) {
    if (p.includes('=')) {
      const [k, v] = p.split('=');
      result[k] = v;
    }
  }

  return result;
}

module.exports = {
  route,
  handleMessage,
  handleCommand,
  parseArgs,
  SYSTEM_PROMPT,
};
