/**
 * Rules engine — loads persistent rules from Airtable and injects into agent prompts
 */
const db = require('./airtable');

// Cache rules for 5 minutes to avoid hitting Airtable on every message
let _cache = {};
const CACHE_TTL = 5 * 60 * 1000;

async function loadRules(agent = null) {
  const key = agent || '_all';
  if (_cache[key] && Date.now() - _cache[key].ts < CACHE_TTL) {
    return _cache[key].rules;
  }

  const records = await db.getRules(agent);
  const formatted = formatRules(records);
  _cache[key] = { rules: formatted, ts: Date.now() };
  return formatted;
}

function formatRules(records) {
  if (!records || !records.length) return '';

  // Group by category
  const grouped = {};
  for (const r of records) {
    const cat = r.Category || 'General';
    if (!grouped[cat]) grouped[cat] = [];
    grouped[cat].push(r.Rule);
  }

  let output = '\n## Active Rules\n';
  for (const [cat, rules] of Object.entries(grouped)) {
    output += `\n### ${cat}\n`;
    for (const rule of rules) {
      output += `- ${rule}\n`;
    }
  }
  return output;
}

async function addRuleFromMessage(message) {
  // Parse: "remember: never use passive voice" -> rule: "never use passive voice"
  // Also: "new rule:", "rule:", "remember this:", "remember that"
  const patterns = [
    /^(?:remember|new rule|rule|remember this|remember that)[:\s]+(.+)/i,
  ];

  let ruleText = message;
  for (const p of patterns) {
    const m = message.match(p);
    if (m) { ruleText = m[1].trim(); break; }
  }

  // Determine agent from content keywords
  let agent = 'all';
  if (/reddit|comment|post|subreddit/i.test(ruleText)) agent = 'reddit';
  else if (/link|outreach|email|brevo|prospect|DR/i.test(ruleText)) agent = 'link-builder';
  else if (/qa|quality|check|review/i.test(ruleText)) agent = 'qa';

  // Determine category
  let category = 'general';
  if (/tone|voice|style|write|language/i.test(ruleText)) category = 'tone';
  else if (/format|template|structure/i.test(ruleText)) category = 'format';
  else if (/process|workflow|step|pipeline/i.test(ruleText)) category = 'process';
  else if (/price|cost|rate|\$/i.test(ruleText)) category = 'pricing';
  else if (/scope|count|number|target/i.test(ruleText)) category = 'scope';
  else if (/quality|standard|check/i.test(ruleText)) category = 'quality';

  await db.addRule(ruleText, agent, 'jeff', category);
  invalidateCache();

  return `Rule saved for *${agent}* [${category}]: "${ruleText}"`;
}

function invalidateCache() {
  _cache = {};
}

module.exports = { loadRules, addRuleFromMessage, invalidateCache };
