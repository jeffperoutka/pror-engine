/**
 * Orchestrator Agent
 * Coordinates all other agents, manages client status, generates reports
 */
const ai = require('../shared/ai');
const db = require('../shared/airtable');
const slack = require('../shared/slack');

const SYSTEM = `You are the PROR Orchestrator. You coordinate link building and Reddit services across clients.
Be concise and data-driven. Format output for Slack using markdown.`;

/**
 * Execute an orchestrator command
 */
async function execute(args) {
  const { action, channel, client } = args;

  switch (action) {
    case 'list-clients':
      return listClients(channel);
    case 'status':
      return clientStatus(client, channel);
    case 'finances':
      return financeSummary(client, channel);
    default:
      // Full sprint orchestration
      return runSprint(client, channel);
  }
}

/**
 * List all active clients with their services
 */
async function listClients(channel) {
  try {
    const clients = await db.getClients();
    if (!clients.length) {
      return slack.post(channel, '📋 No active clients found. Use `/clients add [name]` to add one.');
    }

    let msg = '📋 *Active Clients*\n\n';
    for (const c of clients) {
      const services = c.Services || 'No services';
      const status = c.Status || 'Active';
      msg += `• *${c.Name}* — ${services} (${status})\n`;
    }
    await slack.post(channel, msg);
  } catch (err) {
    await slack.post(channel, `❌ Error fetching clients: ${err.message}`);
  }
}

/**
 * Get status for a specific client
 */
async function clientStatus(clientName, channel) {
  if (!clientName) {
    return slack.post(channel, '⚠️ Please specify a client: `/status [client-name]`');
  }

  try {
    const client = await db.getClient(clientName);
    if (!client) {
      return slack.post(channel, `❌ Client "${clientName}" not found.`);
    }

    const month = new Date().toISOString().slice(0, 7);
    const [links, reddit, finances] = await Promise.all([
      db.getLinks(clientName, { month }),
      db.getReddit(clientName),
      db.getFinances({ client: clientName, month }),
    ]);

    const revenue = finances.filter(f => f.Type === 'Revenue').reduce((s, f) => s + (f.Amount || 0), 0);
    const costs = finances.filter(f => f.Type === 'Cost').reduce((s, f) => s + (f.Amount || 0), 0);

    let msg = `📊 *${client.Name} — ${month} Status*\n\n`;
    msg += `*Services:* ${client.Services || 'N/A'}\n`;
    msg += `*Links:* ${links.length} placed this month\n`;
    msg += `*Reddit:* ${reddit.filter(r => r.Status === 'Posted').length} posted, ${reddit.filter(r => r.Status === 'Drafted').length} drafted\n`;
    msg += `\n💰 *Financials:*\n`;
    msg += `Revenue: $${revenue.toLocaleString()}\n`;
    msg += `Costs: $${costs.toLocaleString()}\n`;
    msg += `Margin: ${revenue ? ((1 - costs / revenue) * 100).toFixed(1) : 0}%\n`;

    await slack.post(channel, msg);
  } catch (err) {
    await slack.post(channel, `❌ Error: ${err.message}`);
  }
}

/**
 * Finance summary across all clients
 */
async function financeSummary(clientName, channel) {
  try {
    const month = new Date().toISOString().slice(0, 7);
    const finances = await db.getFinances({ client: clientName || undefined, month });

    const byClient = {};
    for (const f of finances) {
      const name = f.Client || 'Unknown';
      if (!byClient[name]) byClient[name] = { revenue: 0, costs: 0 };
      if (f.Type === 'Revenue') byClient[name].revenue += f.Amount || 0;
      if (f.Type === 'Cost') byClient[name].costs += f.Amount || 0;
    }

    let msg = `💰 *Financial Summary — ${month}*\n\n`;
    let totalRev = 0, totalCost = 0;

    for (const [name, data] of Object.entries(byClient)) {
      const margin = data.revenue ? ((1 - data.costs / data.revenue) * 100).toFixed(1) : 0;
      msg += `*${name}:* $${data.revenue.toLocaleString()} rev / $${data.costs.toLocaleString()} cost / ${margin}% margin\n`;
      totalRev += data.revenue;
      totalCost += data.costs;
    }

    const totalMargin = totalRev ? ((1 - totalCost / totalRev) * 100).toFixed(1) : 0;
    msg += `\n*TOTAL:* $${totalRev.toLocaleString()} rev / $${totalCost.toLocaleString()} cost / ${totalMargin}% margin`;

    await slack.post(channel, msg);
  } catch (err) {
    await slack.post(channel, `❌ Error: ${err.message}`);
  }
}

/**
 * Run a full sprint for a client
 */
async function runSprint(clientName, channel) {
  if (!clientName) {
    return slack.post(channel, '⚠️ Please specify a client: `/sprint [client-name]`');
  }

  const client = await db.getClient(clientName);
  if (!client) {
    return slack.post(channel, `❌ Client "${clientName}" not found.`);
  }

  await slack.post(channel, `🚀 *Starting sprint for ${client.Name}*\n\nChecking scope and triggering agents...`);

  // TODO: Read scope from client profile, trigger link-builder and reddit agents
  await slack.post(channel, `📋 Sprint orchestration coming in next update. For now, use individual commands:\n• \`/links ${clientName} [count]\`\n• \`/reddit ${clientName} [count]\``);
}

module.exports = { execute };
