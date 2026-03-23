/**
 * Shared Airtable data layer — single source of truth for all agents
 */
const Airtable = require('airtable');

let _base;
function getBase() {
  if (!_base) {
    Airtable.configure({ apiKey: process.env.AIRTABLE_API_KEY });
    _base = Airtable.base(process.env.AIRTABLE_BASE_ID);
  }
  return _base;
}

// ========== CLIENTS ==========

/**
 * Get all active clients
 */
async function getClients() {
  const base = getBase();
  const records = await base('Clients').select({
    filterByFormula: '{Status} = "Active"',
  }).all();
  return records.map(r => ({ id: r.id, ...r.fields }));
}

/**
 * Get a single client by name (case-insensitive)
 */
async function getClient(name) {
  const base = getBase();
  const records = await base('Clients').select({
    filterByFormula: `LOWER({Name}) = LOWER("${name.replace(/"/g, '\\"')}")`,
    maxRecords: 1,
  }).all();
  return records[0] ? { id: records[0].id, ...records[0].fields } : null;
}

/**
 * Create or update a client
 */
async function upsertClient(name, fields) {
  const existing = await getClient(name);
  const base = getBase();
  if (existing) {
    const updated = await base('Clients').update(existing.id, fields);
    return { id: updated.id, ...updated.fields };
  }
  const created = await base('Clients').create({ Name: name, ...fields });
  return { id: created.id, ...created.fields };
}

// ========== LINKS ==========

/**
 * Log a link placement
 */
async function logLink(data) {
  const base = getBase();
  const record = await base('Links').create({
    Client: data.client,
    URL: data.url,
    DR: data.dr || 0,
    'Anchor Text': data.anchor || '',
    'Target Page': data.targetPage || '',
    Status: data.status || 'Placed',
    'Date Placed': data.date || new Date().toISOString().split('T')[0],
    Cost: data.cost || 0,
  });
  return { id: record.id, ...record.fields };
}

/**
 * Get links for a client
 */
async function getLinks(clientName, opts = {}) {
  const base = getBase();
  let formula = `{Client} = "${clientName.replace(/"/g, '\\"')}"`;
  if (opts.month) {
    formula = `AND(${formula}, DATETIME_FORMAT({Date Placed}, 'YYYY-MM') = "${opts.month}")`;
  }
  const records = await base('Links').select({ filterByFormula: formula }).all();
  return records.map(r => ({ id: r.id, ...r.fields }));
}

// ========== REDDIT ==========

/**
 * Log reddit activity
 */
async function logReddit(data) {
  const base = getBase();
  const record = await base('Reddit').create({
    Client: data.client,
    Type: data.type || 'Comment',
    Content: data.content,
    Thread: data.thread || '',
    Subreddit: data.subreddit || '',
    Status: data.status || 'Drafted',
    Date: data.date || new Date().toISOString().split('T')[0],
  });
  return { id: record.id, ...record.fields };
}

/**
 * Get reddit activity for a client
 */
async function getReddit(clientName, opts = {}) {
  const base = getBase();
  let formula = `{Client} = "${clientName.replace(/"/g, '\\"')}"`;
  if (opts.status) {
    formula = `AND(${formula}, {Status} = "${opts.status}")`;
  }
  const records = await base('Reddit').select({ filterByFormula: formula }).all();
  return records.map(r => ({ id: r.id, ...r.fields }));
}

// ========== CAMPAIGNS ==========

/**
 * Log an outreach campaign
 */
async function logCampaign(data) {
  const base = getBase();
  const record = await base('Campaigns').create({
    Client: data.client,
    'Campaign Name': data.name,
    'Brevo ID': data.brevoId || '',
    'Emails Sent': data.sent || 0,
    'Open Rate': data.openRate || 0,
    'Reply Rate': data.replyRate || 0,
    Status: data.status || 'Active',
    Date: data.date || new Date().toISOString().split('T')[0],
  });
  return { id: record.id, ...record.fields };
}

// ========== FINANCES ==========

/**
 * Log a financial transaction
 */
async function logFinance(data) {
  const base = getBase();
  const record = await base('Finances').create({
    Client: data.client,
    Type: data.type, // 'Revenue' or 'Cost'
    Category: data.category, // 'Link Fee', 'Engain', 'Brevo', etc.
    Amount: data.amount,
    Description: data.description || '',
    Date: data.date || new Date().toISOString().split('T')[0],
  });
  return { id: record.id, ...record.fields };
}

/**
 * Get financial summary for a client or all clients
 */
async function getFinances(opts = {}) {
  const base = getBase();
  let formula = '';
  if (opts.client) {
    formula = `{Client} = "${opts.client.replace(/"/g, '\\"')}"`;
  }
  if (opts.month) {
    const monthFilter = `DATETIME_FORMAT({Date}, 'YYYY-MM') = "${opts.month}"`;
    formula = formula ? `AND(${formula}, ${monthFilter})` : monthFilter;
  }
  const records = await base('Finances').select({
    filterByFormula: formula || undefined,
  }).all();
  return records.map(r => ({ id: r.id, ...r.fields }));
}

// ========== RULES ==========

/**
 * Get rules, optionally filtered by agent
 */
async function getRules(agent = null) {
  const base = getBase();
  let filterByFormula;
  if (agent) {
    filterByFormula = `OR({Agent}="all", {Agent}="${agent.replace(/"/g, '\\"')}")`;
  }
  const records = await base('Rules').select({
    filterByFormula: filterByFormula || undefined,
  }).all();
  return records.map(r => ({ id: r.id, ...r.fields }));
}

/**
 * Add a new rule
 */
async function addRule(rule, agent = 'all', source = 'jeff', category = 'general') {
  const base = getBase();
  const record = await base('Rules').create({
    Rule: rule,
    Agent: agent,
    Source: source,
    Category: category,
    CreatedAt: new Date().toISOString(),
  });
  return { id: record.id, ...record.fields };
}

/**
 * Delete a rule by Airtable record ID
 */
async function deleteRule(ruleId) {
  const base = getBase();
  await base('Rules').destroy(ruleId);
  return { deleted: true, id: ruleId };
}

module.exports = {
  getBase,
  getClients,
  getClient,
  upsertClient,
  logLink,
  getLinks,
  logReddit,
  getReddit,
  logCampaign,
  logFinance,
  getFinances,
  getRules,
  addRule,
  deleteRule,
};
