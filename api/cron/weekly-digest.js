/**
 * weekly-digest.js — Comprehensive Friday performance report for PROR
 *
 * Runs every Friday at 6 AM UTC (8 AM SAST) via Vercel cron.
 * Posts a full business snapshot to Slack #command-center.
 *
 * Data sources:
 *   1. Brevo API — campaign stats (sent, opens, clicks, bounces) per sender domain
 *   2. Airtable Negotiations — deals, costs, links placed
 *   3. Airtable SenderHealth — historical metrics for trend comparison
 *   4. Airtable Outreach — prospect pipeline counts
 *   5. Airtable AB Tests — best performing variants
 */
const { waitUntil } = require('@vercel/functions');
const slack = require('../../shared/slack');
const airtable = require('../../shared/airtable');

const BREVO_KEY = process.env.BREVO_API_KEY;
const AIRTABLE_PAT = process.env.AIRTABLE_PAT;
const AIRTABLE_BASE = process.env.AIRTABLE_BASE;
const CHANNEL = () => process.env.CHANNEL_COMMAND_CENTER;

// ── Client → Sender Domain mapping (mirrors sender-health.js) ───────────────

const CLIENT_DOMAINS = {
  'Able Ammo':      ['ableammo.ecom-ranker.com', 'ecom-ranker.com'],
  'Dr. Dabber':     ['drdabber.ecom-guestposts.com', 'ecom-guestposts.com'],
  'Felina':         ['felina.guest-poster.com', 'guest-poster.com'],
  'Mill Packaging': ['millpkg.linkinsertion.live', 'linkinsertion.live'],
  'PrimeRx':        ['primerx.guestpost-now.com', 'guestpost-now.com'],
  'SMOKEA':         ['smokea.guest-post.live', 'guest-post.live'],
  'MrSkin':         ['mrskin.ecom-links.com', 'ecom-links.com'],
  'VRAI':           ['vrai.linkinsertion.us', 'linkinsertion.us'],
  'AMS Fulfillment':['amsfulfillment.ecom-ranker.com'],
  'Built Bar':      ['builtbar.ecom-guestposts.com'],
  'NutraBio':       ['nutrabio.guest-poster.com'],
  'Vivante Living': ['vivante.linkinsertion.live'],
  'Goodr':          ['goodr.guestpost-now.com'],
};

const ALL_DOMAINS = Object.values(CLIENT_DOMAINS).flat();

// ── Date helpers ─────────────────────────────────────────────────────────────

function weekRange(weeksAgo = 0) {
  const now = new Date();
  const end = new Date(now);
  end.setDate(end.getDate() - (weeksAgo * 7));
  const start = new Date(end);
  start.setDate(start.getDate() - 7);
  return {
    start: start.toISOString().split('T')[0],
    end: end.toISOString().split('T')[0],
    startDate: start,
    endDate: end,
  };
}

function isoDate(d) { return d.toISOString().split('T')[0]; }

// ── Trend formatting ─────────────────────────────────────────────────────────

function trend(current, previous) {
  if (previous === null || previous === undefined || previous === 0) {
    if (current === 0) return '➡️ → 0%';
    return '🆕';
  }
  const pct = ((current - previous) / previous * 100).toFixed(1);
  const arrow = current > previous ? '↑' : current < previous ? '↓' : '→';
  const emoji = current > previous ? '📈' : current < previous ? '📉' : '➡️';
  return `${emoji} ${arrow} ${Math.abs(pct)}%`;
}

function fmtPct(n, total) {
  if (!total || total === 0) return '0%';
  return (n / total * 100).toFixed(1) + '%';
}

function fmtNum(n) {
  if (n === null || n === undefined) return '0';
  return n.toLocaleString('en-US');
}

function fmtMoney(n) {
  if (n === null || n === undefined) return '$0';
  return '$' + n.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
}

function padRight(str, len) { return (str || '').slice(0, len).padEnd(len); }
function padLeft(str, len) { return (str || '').slice(0, len).padStart(len); }

// ── Brevo API ────────────────────────────────────────────────────────────────

async function brevoGet(path) {
  const res = await fetch(`https://api.brevo.com/v3${path}`, {
    headers: { 'api-key': BREVO_KEY, 'Content-Type': 'application/json' },
  });
  if (!res.ok) {
    const text = await res.text();
    console.error(`[weekly-digest] Brevo error ${res.status}: ${text.slice(0, 200)}`);
    return {};
  }
  return res.json();
}

/**
 * Fetch all sent campaigns within a date range, grouped by client.
 * Returns { clientName: { sent, delivered, opens, replies, bounces, clicks } }
 */
async function getCampaignStatsByClient(startDate, endDate) {
  let allCampaigns = [];
  let offset = 0;
  const limit = 50;
  let hasMore = true;

  while (hasMore) {
    const data = await brevoGet(
      `/emailCampaigns?status=sent&limit=${limit}&offset=${offset}&startDate=${startDate}&endDate=${endDate}`
    );
    const campaigns = data.campaigns || [];
    allCampaigns.push(...campaigns);
    hasMore = campaigns.length === limit;
    offset += limit;
  }

  // Map sender email -> client name
  const emailToClient = {};
  for (const [client, domains] of Object.entries(CLIENT_DOMAINS)) {
    for (const domain of domains) {
      emailToClient[domain] = client;
    }
  }

  // Aggregate per client
  const byClient = {};
  for (const c of allCampaigns) {
    const senderEmail = c.sender?.email || '';
    const senderDomain = senderEmail.split('@')[1] || '';

    let clientName = emailToClient[senderDomain] || null;
    // Also check subdomain match
    if (!clientName) {
      for (const [client, domains] of Object.entries(CLIENT_DOMAINS)) {
        if (domains.some(d => senderEmail.endsWith('@' + d))) {
          clientName = client;
          break;
        }
      }
    }
    if (!clientName) continue;

    if (!byClient[clientName]) {
      byClient[clientName] = { sent: 0, delivered: 0, opens: 0, clicks: 0, hardBounces: 0, softBounces: 0, complaints: 0, replies: 0, campaigns: 0 };
    }

    const s = c.statistics?.globalStats || c.statistics || {};
    byClient[clientName].sent += s.sent || 0;
    byClient[clientName].delivered += s.delivered || 0;
    byClient[clientName].opens += s.uniqueOpens || 0;
    byClient[clientName].clicks += s.uniqueClicks || 0;
    byClient[clientName].hardBounces += s.hardBounces || 0;
    byClient[clientName].softBounces += s.softBounces || 0;
    byClient[clientName].complaints += s.complaints || s.spamReports || 0;
    byClient[clientName].replies += s.uniqueReplies || 0;
    byClient[clientName].campaigns += 1;
  }

  return byClient;
}

// ── Airtable helpers ─────────────────────────────────────────────────────────

function airtableFetch(table, opts = {}) {
  const params = new URLSearchParams();
  if (opts.filterByFormula) params.set('filterByFormula', opts.filterByFormula);
  if (opts.sort) {
    opts.sort.forEach((s, i) => {
      params.set(`sort[${i}][field]`, s.field);
      params.set(`sort[${i}][direction]`, s.direction || 'asc');
    });
  }
  if (opts.pageSize) params.set('pageSize', String(opts.pageSize));

  const qs = params.toString() ? `?${params.toString()}` : '';
  return fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE}/${encodeURIComponent(table)}${qs}`, {
    headers: { 'Authorization': `Bearer ${AIRTABLE_PAT}` },
  }).then(r => r.json()).then(data => {
    return (data.records || []).map(r => ({ id: r.id, ...r.fields }));
  }).catch(err => {
    console.error(`[weekly-digest] Airtable ${table} error:`, err.message);
    return [];
  });
}

/**
 * Get negotiations for a date range.
 */
async function getNegotiationsForWeek(startDate, endDate) {
  return airtableFetch('Negotiations', {
    filterByFormula: `AND(IS_AFTER({Date}, "${startDate}"), IS_BEFORE({Date}, "${endDate}T23:59:59"))`,
  });
}

/**
 * Get outreach pipeline counts per client.
 */
async function getOutreachPipeline() {
  const records = await airtableFetch('Outreach', { pageSize: 100 });
  const byClient = {};
  for (const r of records) {
    const client = r.Client || 'Unknown';
    if (!byClient[client]) {
      byClient[client] = { total: 0, contacted: 0, replied: 0, negotiating: 0, placed: 0 };
    }
    byClient[client].total++;
    if (r.Status === 'Contacted' || r.Status === 'Sent') byClient[client].contacted++;
    if (r['Reply Status'] && r['Reply Status'] !== 'none') byClient[client].replied++;
    if (r.Status === 'Negotiating' || r['Negotiation Round'] > 0) byClient[client].negotiating++;
    if (r.Status === 'Placed' || r.Status === 'Price Confirmed') byClient[client].placed++;
  }
  return byClient;
}

/**
 * Get links placed from Airtable for a date range.
 */
async function getLinksForWeek(startDate, endDate) {
  return airtableFetch('Links', {
    filterByFormula: `AND(IS_AFTER({Date Placed}, "${startDate}"), IS_BEFORE({Date Placed}, "${endDate}T23:59:59"))`,
  });
}

/**
 * Get historical SenderHealth records for trend comparison.
 */
async function getSenderHealthHistory(startDate, endDate) {
  return airtableFetch('SenderHealth', {
    filterByFormula: `AND(IS_AFTER({Date}, "${startDate}"), IS_BEFORE({Date}, "${endDate}T23:59:59"))`,
  });
}

/**
 * Get running A/B tests with results.
 */
async function getABTestInsights() {
  try {
    const tests = await airtable.getABTests('running');
    const completed = await airtable.getABTests('completed');
    // Get recently completed tests (last 7 days)
    const recent = completed.filter(t => {
      const d = new Date(t.StartDate || 0);
      const weekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
      return d >= weekAgo;
    });
    return [...tests, ...recent];
  } catch {
    return [];
  }
}

// ── Prospect inventory check (via Brevo scheduled campaigns) ─────────────────

async function getProspectInventory() {
  const CLIENT_SLUGS = {
    'Able Ammo': 'ABLE-AMMO',
    'Dr. Dabber': 'DR-DABBER',
    'Felina': 'FELINA',
    'Mill Packaging': 'MILL-PACKAGING',
    'PrimeRx': 'PRIMERX',
    'SMOKEA': 'SMOKEA',
    'MrSkin': 'MRSKIN',
    'VRAI': 'VRAI',
    'AMS Fulfillment': 'AMS-FULFILLMENT',
    'Built Bar': 'BUILT-BAR',
    'NutraBio': 'NUTRABIO',
    'Vivante Living': 'VIVANTE-LIVING',
    'Goodr': 'GOODR',
  };

  // Get scheduled campaigns to estimate remaining prospects
  let scheduledCampaigns = [];
  let offset = 0;
  const limit = 50;
  let hasMore = true;

  while (hasMore) {
    const data = await brevoGet(`/emailCampaigns?type=classic&status=scheduled&limit=${limit}&offset=${offset}&sort=desc`);
    const campaigns = data.campaigns || [];
    scheduledCampaigns.push(...campaigns);
    hasMore = campaigns.length === limit;
    offset += limit;
  }

  const inventory = {};
  for (const [clientName, slug] of Object.entries(CLIENT_SLUGS)) {
    // Count SEQ1 campaigns only (each represents a unique prospect batch)
    const seq1Campaigns = scheduledCampaigns.filter(c =>
      c.name && c.name.startsWith(slug) && c.name.includes('-SEQ1-')
    );
    let remaining = 0;
    for (const c of seq1Campaigns) {
      // Estimate from campaign recipient count
      remaining += c.recipients?.estimatedCount || 0;
    }
    inventory[clientName] = remaining;
  }

  return inventory;
}

// ── Main digest builder ──────────────────────────────────────────────────────

async function run() {
  const thisWeek = weekRange(0);
  const lastWeek = weekRange(1);

  console.error(`[weekly-digest] Building report: ${thisWeek.start} to ${thisWeek.end}`);

  // Fetch all data in parallel
  const [
    thisWeekStats,
    lastWeekStats,
    thisWeekNegotiations,
    lastWeekNegotiations,
    thisWeekLinks,
    lastWeekLinks,
    pipeline,
    abTests,
    inventory,
    senderHealthThis,
    senderHealthLast,
  ] = await Promise.all([
    getCampaignStatsByClient(thisWeek.start, thisWeek.end),
    getCampaignStatsByClient(lastWeek.start, lastWeek.end),
    getNegotiationsForWeek(thisWeek.start, thisWeek.end),
    getNegotiationsForWeek(lastWeek.start, lastWeek.end),
    getLinksForWeek(thisWeek.start, thisWeek.end),
    getLinksForWeek(lastWeek.start, lastWeek.end),
    getOutreachPipeline().catch(() => ({})),
    getABTestInsights(),
    getProspectInventory().catch(() => ({})),
    getSenderHealthHistory(thisWeek.start, thisWeek.end).catch(() => []),
    getSenderHealthHistory(lastWeek.start, lastWeek.end).catch(() => []),
  ]);

  // ── Compute aggregates ──

  const clients = Object.keys(CLIENT_DOMAINS);

  // Totals for this week
  const totals = { sent: 0, delivered: 0, opens: 0, clicks: 0, bounces: 0, replies: 0, complaints: 0 };
  const lastTotals = { sent: 0, delivered: 0, opens: 0, clicks: 0, bounces: 0, replies: 0, complaints: 0 };

  for (const client of clients) {
    const s = thisWeekStats[client] || {};
    totals.sent += s.sent || 0;
    totals.delivered += s.delivered || 0;
    totals.opens += s.opens || 0;
    totals.clicks += s.clicks || 0;
    totals.bounces += (s.hardBounces || 0) + (s.softBounces || 0);
    totals.replies += s.replies || 0;
    totals.complaints += s.complaints || 0;

    const ls = lastWeekStats[client] || {};
    lastTotals.sent += ls.sent || 0;
    lastTotals.delivered += ls.delivered || 0;
    lastTotals.opens += ls.opens || 0;
    lastTotals.clicks += ls.clicks || 0;
    lastTotals.bounces += (ls.hardBounces || 0) + (ls.softBounces || 0);
    lastTotals.replies += ls.replies || 0;
    lastTotals.complaints += ls.complaints || 0;
  }

  // Negotiation metrics
  const activeNegotiations = thisWeekNegotiations.filter(n => n.Action !== 'decline' && n.Action !== 'ignore');
  const closedDeals = thisWeekNegotiations.filter(n => n.Action === 'accept' || n.Action === 'price_confirmed');
  const lastClosedDeals = lastWeekNegotiations.filter(n => n.Action === 'accept' || n.Action === 'price_confirmed');

  // Link costs and revenue
  const thisWeekCost = thisWeekLinks.reduce((s, l) => s + (l.Cost || 0), 0);
  const lastWeekCost = lastWeekLinks.reduce((s, l) => s + (l.Cost || 0), 0);

  // Estimate revenue from links (avg client charge ~$350/link as baseline)
  const AVG_CLIENT_CHARGE = 350;
  const thisWeekRevenue = thisWeekLinks.length * AVG_CLIENT_CHARGE;
  const lastWeekRevenue = lastWeekLinks.length * AVG_CLIENT_CHARGE;
  const thisWeekProfit = thisWeekRevenue - thisWeekCost;
  const lastWeekProfit = lastWeekRevenue - lastWeekCost;
  const thisMargin = thisWeekRevenue > 0 ? (thisWeekProfit / thisWeekRevenue * 100) : 0;
  const lastMargin = lastWeekRevenue > 0 ? (lastWeekProfit / lastWeekRevenue * 100) : 0;
  const avgLinkCost = thisWeekLinks.length > 0 ? thisWeekCost / thisWeekLinks.length : 0;
  const lastAvgLinkCost = lastWeekLinks.length > 0 ? lastWeekCost / lastWeekLinks.length : 0;

  // Avg deal price from negotiations
  const avgDealPrice = closedDeals.length > 0
    ? closedDeals.reduce((s, n) => s + (n.OurOffer || n.TheirPrice || 0), 0) / closedDeals.length
    : 0;

  // ── Sender health warnings ──
  const domainWarnings = [];
  for (const rec of senderHealthThis) {
    if (rec.Status === 'critical') {
      domainWarnings.push({ domain: rec.Domain, status: 'critical', openRate: rec.OpenRate });
    } else if (rec.Status === 'warning') {
      domainWarnings.push({ domain: rec.Domain, status: 'warning', openRate: rec.OpenRate });
    }
  }

  // ── Best / worst client by reply rate ──
  let bestClient = null;
  let worstClient = null;
  let bestReplyRate = -1;
  let worstReplyRate = Infinity;

  for (const client of clients) {
    const s = thisWeekStats[client];
    if (!s || s.sent === 0) continue;
    const replyRate = (s.replies || 0) / s.sent;
    if (replyRate > bestReplyRate) { bestReplyRate = replyRate; bestClient = client; }
    if (replyRate < worstReplyRate) { worstReplyRate = replyRate; worstClient = client; }
  }

  // ── Low inventory alerts ──
  const lowInventory = [];
  for (const [client, remaining] of Object.entries(inventory)) {
    if (remaining < 100) {
      lowInventory.push({ client, remaining });
    }
  }
  lowInventory.sort((a, b) => a.remaining - b.remaining);

  // ── A/B test winner ──
  let abInsight = null;
  if (abTests.length > 0) {
    // Find test with biggest result differential
    const byTest = {};
    for (const t of abTests) {
      const name = t.TestName || 'Unknown';
      if (!byTest[name]) byTest[name] = [];
      byTest[name].push(t);
    }
    let bestDiff = 0;
    for (const [name, variants] of Object.entries(byTest)) {
      if (variants.length >= 2) {
        const sorted = variants.sort((a, b) => (b.Result || 0) - (a.Result || 0));
        const diff = (sorted[0].Result || 0) - (sorted[sorted.length - 1].Result || 0);
        if (diff > bestDiff) {
          bestDiff = diff;
          abInsight = {
            name,
            winner: sorted[0].Variant || 'A',
            metric: sorted[0].Metric || 'open_rate',
            improvement: diff,
          };
        }
      }
    }
  }

  // ── Build Slack Block Kit message ──────────────────────────────────────────

  const weekOf = new Date(thisWeek.start).toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });
  const blocks = [];

  // Header
  blocks.push({
    type: 'header',
    text: { type: 'plain_text', text: `📊 PROR Weekly Digest — Week of ${weekOf}` },
  });

  // ━━━ SECTION 1: OUTREACH VOLUME ━━━
  blocks.push({ type: 'divider' });
  blocks.push({
    type: 'section',
    text: { type: 'mrkdwn', text: '*━━━ 📬 OUTREACH VOLUME ━━━*' },
  });

  let outreachTable = '```\n';
  outreachTable += `${padRight('Client', 16)} ${padLeft('Sent', 6)} ${padLeft('Opens', 12)} ${padLeft('Replies', 12)} ${padLeft('Bounce', 12)}\n`;
  outreachTable += `${'─'.repeat(16)} ${'─'.repeat(6)} ${'─'.repeat(12)} ${'─'.repeat(12)} ${'─'.repeat(12)}\n`;

  for (const client of clients) {
    const s = thisWeekStats[client] || {};
    const sent = s.sent || 0;
    const opens = s.opens || 0;
    const replies = s.replies || 0;
    const bounces = (s.hardBounces || 0) + (s.softBounces || 0);
    if (sent === 0 && opens === 0) continue;

    const shortName = client.length > 15 ? client.slice(0, 13) + '..' : client;
    outreachTable += `${padRight(shortName, 16)} ${padLeft(fmtNum(sent), 6)} ${padLeft(`${fmtNum(opens)}(${fmtPct(opens, sent)})`, 12)} ${padLeft(`${fmtNum(replies)}(${fmtPct(replies, sent)})`, 12)} ${padLeft(`${fmtNum(bounces)}(${fmtPct(bounces, sent)})`, 12)}\n`;
  }

  outreachTable += `${'─'.repeat(16)} ${'─'.repeat(6)} ${'─'.repeat(12)} ${'─'.repeat(12)} ${'─'.repeat(12)}\n`;
  outreachTable += `${padRight('TOTAL', 16)} ${padLeft(fmtNum(totals.sent), 6)} ${padLeft(`${fmtNum(totals.opens)}(${fmtPct(totals.opens, totals.sent)})`, 12)} ${padLeft(`${fmtNum(totals.replies)}(${fmtPct(totals.replies, totals.sent)})`, 12)} ${padLeft(`${fmtNum(totals.bounces)}(${fmtPct(totals.bounces, totals.sent)})`, 12)}\n`;
  outreachTable += '```';

  blocks.push({ type: 'section', text: { type: 'mrkdwn', text: outreachTable } });

  // ━━━ SECTION 2: RESPONSE METRICS (WoW comparison) ━━━
  blocks.push({ type: 'divider' });
  blocks.push({
    type: 'section',
    text: { type: 'mrkdwn', text: '*━━━ 📩 RESPONSE METRICS (vs last week) ━━━*' },
  });

  let responseLines = [];
  for (const client of clients) {
    const s = thisWeekStats[client] || {};
    const ls = lastWeekStats[client] || {};
    if (!s.sent && !ls.sent) continue;

    const openRate = s.delivered > 0 ? (s.opens / s.delivered * 100) : 0;
    const lastOpenRate = ls.delivered > 0 ? (ls.opens / ls.delivered * 100) : 0;
    const replyRate = s.sent > 0 ? (s.replies / s.sent * 100) : 0;
    const lastReplyRate = ls.sent > 0 ? (ls.replies / ls.sent * 100) : 0;
    const bounceRate = s.sent > 0 ? ((s.hardBounces || 0) + (s.softBounces || 0)) / s.sent * 100 : 0;

    const openTrend = openRate > lastOpenRate ? '↑' : openRate < lastOpenRate ? '↓' : '→';
    const replyTrend = replyRate > lastReplyRate ? '↑' : replyRate < lastReplyRate ? '↓' : '→';

    responseLines.push(`*${client}:* Open ${openRate.toFixed(1)}% ${openTrend} · Reply ${replyRate.toFixed(1)}% ${replyTrend} · Bounce ${bounceRate.toFixed(1)}%`);
  }

  if (responseLines.length > 0) {
    blocks.push({ type: 'section', text: { type: 'mrkdwn', text: responseLines.join('\n') } });
  } else {
    blocks.push({ type: 'section', text: { type: 'mrkdwn', text: '_No response data this week_' } });
  }

  // ━━━ SECTION 3: NEGOTIATIONS ━━━
  blocks.push({ type: 'divider' });
  blocks.push({
    type: 'section',
    text: { type: 'mrkdwn', text: '*━━━ 💰 NEGOTIATIONS ━━━*' },
  });

  const negText = [
    `*Active:* ${activeNegotiations.length}  |  *Closed:* ${closedDeals.length}  |  *Links Placed:* ${thisWeekLinks.length}`,
    `*Avg Deal Price:* ${fmtMoney(avgDealPrice)}  |  *Avg Link Cost:* ${fmtMoney(avgLinkCost)}`,
    `*Revenue:* ${fmtMoney(thisWeekRevenue)}  |  *Cost:* ${fmtMoney(thisWeekCost)}  |  *Margin:* ${thisMargin.toFixed(0)}%`,
  ].join('\n');

  blocks.push({ type: 'section', text: { type: 'mrkdwn', text: negText } });

  // ━━━ SECTION 4: PIPELINE STATUS ━━━
  blocks.push({ type: 'divider' });
  blocks.push({
    type: 'section',
    text: { type: 'mrkdwn', text: '*━━━ 🔄 PIPELINE STATUS ━━━*' },
  });

  let pipelineTable = '```\n';
  pipelineTable += `${padRight('Client', 16)} ${padLeft('DB', 5)} ${padLeft('Cntct', 5)} ${padLeft('Reply', 5)} ${padLeft('Negot', 5)} ${padLeft('Placed', 6)} ${padLeft('Inv', 5)}\n`;
  pipelineTable += `${'─'.repeat(16)} ${'─'.repeat(5)} ${'─'.repeat(5)} ${'─'.repeat(5)} ${'─'.repeat(5)} ${'─'.repeat(6)} ${'─'.repeat(5)}\n`;

  for (const client of clients) {
    const p = pipeline[client] || { total: 0, contacted: 0, replied: 0, negotiating: 0, placed: 0 };
    const inv = inventory[client] || 0;
    const shortName = client.length > 15 ? client.slice(0, 13) + '..' : client;
    pipelineTable += `${padRight(shortName, 16)} ${padLeft(String(p.total), 5)} ${padLeft(String(p.contacted), 5)} ${padLeft(String(p.replied), 5)} ${padLeft(String(p.negotiating), 5)} ${padLeft(String(p.placed), 6)} ${padLeft(String(inv), 5)}\n`;
  }
  pipelineTable += '```';

  blocks.push({ type: 'section', text: { type: 'mrkdwn', text: pipelineTable } });

  // ━━━ SECTION 5: TRENDS (Week over Week) ━━━
  blocks.push({ type: 'divider' });
  blocks.push({
    type: 'section',
    text: { type: 'mrkdwn', text: '*━━━ 📈 TRENDS (vs last week) ━━━*' },
  });

  const thisOpenRate = totals.delivered > 0 ? (totals.opens / totals.delivered * 100) : 0;
  const lastOpenRate = lastTotals.delivered > 0 ? (lastTotals.opens / lastTotals.delivered * 100) : 0;
  const thisReplyRate = totals.sent > 0 ? (totals.replies / totals.sent * 100) : 0;
  const lastReplyRate = lastTotals.sent > 0 ? (lastTotals.replies / lastTotals.sent * 100) : 0;

  const trendLines = [
    `Emails sent:    ${fmtNum(lastTotals.sent)} → ${fmtNum(totals.sent)}  ${trend(totals.sent, lastTotals.sent)}`,
    `Open rate:      ${lastOpenRate.toFixed(1)}% → ${thisOpenRate.toFixed(1)}%  ${trend(thisOpenRate, lastOpenRate)}`,
    `Reply rate:     ${lastReplyRate.toFixed(1)}% → ${thisReplyRate.toFixed(1)}%  ${trend(thisReplyRate, lastReplyRate)}`,
    `Links placed:   ${lastWeekLinks.length} → ${thisWeekLinks.length}  ${trend(thisWeekLinks.length, lastWeekLinks.length)}`,
    `Avg link cost:  ${fmtMoney(lastAvgLinkCost)} → ${fmtMoney(avgLinkCost)}  ${trend(avgLinkCost, lastAvgLinkCost)}`,
    `Revenue:        ${fmtMoney(lastWeekRevenue)} → ${fmtMoney(thisWeekRevenue)}  ${trend(thisWeekRevenue, lastWeekRevenue)}`,
    `Profit margin:  ${lastMargin.toFixed(0)}% → ${thisMargin.toFixed(0)}%  ${trend(thisMargin, lastMargin)}`,
  ];

  blocks.push({ type: 'section', text: { type: 'mrkdwn', text: '```\n' + trendLines.join('\n') + '\n```' } });

  // ━━━ SECTION 6: HIGHLIGHTS & ALERTS ━━━
  blocks.push({ type: 'divider' });
  blocks.push({
    type: 'section',
    text: { type: 'mrkdwn', text: '*━━━ 🏆 HIGHLIGHTS & ALERTS ━━━*' },
  });

  const highlights = [];

  if (bestClient && bestReplyRate > 0) {
    highlights.push(`🥇 *Best client:* ${bestClient} (${(bestReplyRate * 100).toFixed(1)}% reply rate)`);
  }
  if (worstClient && worstReplyRate < Infinity && worstClient !== bestClient) {
    highlights.push(`⚠️ *Watch:* ${worstClient} (${(worstReplyRate * 100).toFixed(1)}% reply rate, lowest)`);
  }
  if (abInsight) {
    const pctStr = typeof abInsight.improvement === 'number' ? `+${abInsight.improvement.toFixed(1)}%` : '';
    highlights.push(`🧪 *A/B winner:* ${abInsight.name} — Variant ${abInsight.winner} ${pctStr} ${abInsight.metric.replace('_', ' ')}`);
  }
  if (domainWarnings.length > 0) {
    for (const w of domainWarnings.slice(0, 3)) {
      const emoji = w.status === 'critical' ? '🔴' : '⚠️';
      highlights.push(`${emoji} *${w.domain}* — ${w.status} (${w.openRate || 0}% open rate)`);
    }
    if (domainWarnings.length > 3) {
      highlights.push(`   _...and ${domainWarnings.length - 3} more domains in warning/critical_`);
    }
  }
  if (lowInventory.length > 0) {
    for (const inv of lowInventory.slice(0, 3)) {
      highlights.push(`📦 *Low inventory:* ${inv.client} (${inv.remaining} prospects left)`);
    }
  }
  if (closedDeals.length > 0) {
    highlights.push(`💰 *Deals closed this week:* ${closedDeals.length} (avg ${fmtMoney(avgDealPrice)})`);
  }

  if (highlights.length > 0) {
    blocks.push({ type: 'section', text: { type: 'mrkdwn', text: highlights.join('\n') } });
  } else {
    blocks.push({ type: 'section', text: { type: 'mrkdwn', text: '_No alerts — first week, baseline being established_' } });
  }

  // Footer
  blocks.push({ type: 'divider' });
  blocks.push({
    type: 'context',
    elements: [{
      type: 'mrkdwn',
      text: `_Generated ${new Date().toLocaleString('en-US', { timeZone: 'Africa/Johannesburg', dateStyle: 'medium', timeStyle: 'short' })} SAST — Data: Brevo + Airtable_`,
    }],
  });

  // Post to Slack
  await slack.postBlocks(
    CHANNEL(),
    blocks,
    `📊 PROR Weekly Digest — Week of ${weekOf}`
  );

  console.error(`[weekly-digest] Posted to Slack. Totals: sent=${totals.sent}, opens=${totals.opens}, replies=${totals.replies}, links=${thisWeekLinks.length}`);

  return {
    weekOf,
    totalSent: totals.sent,
    totalOpens: totals.opens,
    totalReplies: totals.replies,
    linksPlaced: thisWeekLinks.length,
    dealsClosed: closedDeals.length,
    revenue: thisWeekRevenue,
    cost: thisWeekCost,
  };
}

// ── Vercel Cron Handler ──────────────────────────────────────────────────────

module.exports = async (req, res) => {
  // Auth check
  const authHeader = req.headers.authorization;
  if (process.env.CRON_SECRET && authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  if (req.method !== 'GET' && req.method !== 'POST') {
    return res.status(405).end();
  }

  waitUntil(
    run()
      .then(async result => {
        console.error(`[weekly-digest] Done:`, JSON.stringify(result));
        await airtable.logCronRun('weekly-digest').catch(() => {});
      })
      .catch(err => console.error('[weekly-digest] Fatal:', err))
  );

  res.status(200).json({ ok: true, message: 'Weekly digest triggered' });
};
