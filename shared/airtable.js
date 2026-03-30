/**
 * Shared Airtable data layer — single source of truth for all agents
 */
const Airtable = require('airtable');

let _base;
function getBase() {
  if (!_base) {
    const key = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
    const baseId = (process.env.AIRTABLE_BASE_ID || process.env.AIRTABLE_BASE || '').trim();
    if (!key) throw new Error('[airtable] No AIRTABLE_API_KEY or AIRTABLE_PAT set');
    if (!baseId) throw new Error('[airtable] No AIRTABLE_BASE_ID or AIRTABLE_BASE set');
    Airtable.configure({ apiKey: key });
    _base = Airtable.base(baseId);
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

// ========== OUTREACH / NEGOTIATIONS ==========

/**
 * Get outreach record by sender domain — includes campaign IDs for drip cancellation
 */
async function getOutreachByDomain(domain) {
  const base = getBase();
  const records = await base('Outreach').select({
    filterByFormula: `LOWER({Domain}) = "${domain.toLowerCase().replace(/"/g, '\\"')}"`,
    maxRecords: 1,
  }).all();
  return records[0] ? { id: records[0].id, ...records[0].fields } : null;
}

/**
 * Get negotiation history for a thread (by domain or thread ID)
 */
async function getNegotiationHistory(domain) {
  const base = getBase();
  try {
    const records = await base('Negotiations').select({
      filterByFormula: `LOWER({Domain}) = "${domain.toLowerCase().replace(/"/g, '\\"')}"`,
      sort: [{ field: 'Date', direction: 'asc' }],
    }).all();
    return records.map(r => ({ id: r.id, ...r.fields }));
  } catch {
    // Table may not exist yet
    return [];
  }
}

/**
 * Log a negotiation event (each email exchange = one record)
 */
async function logNegotiation(data) {
  const base = getBase();
  try {
    const record = await base('Negotiations').create({
      Domain: data.domain,
      Client: data.client || '',
      Round: data.round || 1,
      Direction: data.direction, // 'inbound' or 'outbound'
      TheirPrice: data.theirPrice || null,
      OurOffer: data.ourOffer || null,
      DR: data.dr || null,
      Sentiment: data.sentiment || 'neutral',
      Action: data.action || '',
      Summary: data.summary || '',
      ThreadId: data.threadId || '',
      AutoReplied: data.autoReplied || false,
      JeffReviewed: false,
      Date: new Date().toISOString(),
    });
    return { id: record.id, ...record.fields };
  } catch (err) {
    console.error('[airtable] logNegotiation failed:', err.message);
    return null;
  }
}

/**
 * Update a negotiation record (e.g. Jeff's feedback)
 */
async function updateNegotiation(recordId, updates) {
  const base = getBase();
  await base('Negotiations').update(recordId, updates);
}

/**
 * Log an A/B test variant and its performance
 */
async function logABTest(data) {
  const base = getBase();
  try {
    const record = await base('AB Tests').create({
      TestName: data.testName,
      Variant: data.variant, // 'A' or 'B'
      Client: data.client || '',
      Metric: data.metric, // 'open_rate', 'reply_rate', 'negotiation_success'
      Subject: data.subject || '',
      BodyPreview: data.bodyPreview || '',
      SampleSize: data.sampleSize || 0,
      Result: data.result || 0,
      StartDate: data.startDate || new Date().toISOString().split('T')[0],
      Status: data.status || 'running',
    });
    return { id: record.id, ...record.fields };
  } catch (err) {
    console.error('[airtable] logABTest failed:', err.message);
    return null;
  }
}

/**
 * Get running A/B tests
 */
async function getABTests(status = 'running') {
  const base = getBase();
  try {
    const records = await base('AB Tests').select({
      filterByFormula: `{Status} = "${status}"`,
    }).all();
    return records.map(r => ({ id: r.id, ...r.fields }));
  } catch {
    return [];
  }
}

// ========== SYSTEM HEALTH ==========

/**
 * Log a cron run to SystemHealth table so the manager can verify crons are running.
 * Upserts by CronName — updates existing record or creates new one.
 */
async function logCronRun(cronName) {
  const base = getBase();
  try {
    // Find existing record for this cron
    const existing = await base('SystemHealth').select({
      filterByFormula: `AND({Type} = "cron_run", {CronName} = "${cronName}")`,
      maxRecords: 1,
    }).all();

    const fields = {
      Type: 'cron_run',
      CronName: cronName,
      Date: new Date().toISOString(),
    };

    if (existing.length > 0) {
      await base('SystemHealth').update(existing[0].id, { Date: fields.Date });
    } else {
      await base('SystemHealth').create(fields);
    }
  } catch (err) {
    // Non-fatal — don't break cron execution for health logging
    console.error(`[airtable] logCronRun(${cronName}) failed:`, err.message);
  }
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
  // Negotiations
  getOutreachByDomain,
  getNegotiationHistory,
  logNegotiation,
  updateNegotiation,
  // A/B Testing
  logABTest,
  getABTests,
  // System Health
  logCronRun,
};
