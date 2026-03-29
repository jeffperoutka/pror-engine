/**
 * manager.js — The Manager Agent: neutral third-party overseer for PROR outreach
 *
 * Runs twice daily (10 AM and 4 PM SAST) via Vercel cron.
 * Also callable manually via GET /api/cron/manager
 *
 * Responsibilities:
 *   1. System health — verify all crons ran on schedule
 *   2. Pipeline integrity — prospect inventory, scheduling gaps, reply processing, stalled negotiations
 *   3. Performance anomaly detection — open/reply/bounce rate deviations
 *   4. Cost efficiency — cost per link, margin erosion alerts
 *   5. Auto-fix — pause senders, emergency replenish, reduce send volume
 *   6. AI-generated improvement suggestions via Claude Haiku
 *   7. Consolidated Slack report to #command-center
 */

const { waitUntil } = require('@vercel/functions');
const slack = require('../../shared/slack');
const { brevoFetch } = require('../../shared/brevo');

const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
const AIRTABLE_PAT = process.env.AIRTABLE_PAT;
const AIRTABLE_BASE = process.env.AIRTABLE_BASE;
const CHANNEL = () => process.env.CHANNEL_COMMAND_CENTER;

// ── Client Configuration ────────────────────────────────────────────────────

const CLIENTS = [
  { slug: 'ABLE-AMMO', name: 'Able Ammo', senderDomains: ['ecom-ranker.com', 'ableammo.ecom-ranker.com'] },
  { slug: 'DR-DABBER', name: 'Dr. Dabber', senderDomains: ['ecom-guestposts.com', 'drdabber.ecom-guestposts.com'] },
  { slug: 'FELINA', name: 'Felina', senderDomains: ['guestpost-now.com', 'felina.guest-poster.com'] },
  { slug: 'MILL-PACKAGING', name: 'Mill Packaging', senderDomains: ['linkinsertion.live', 'millpkg.linkinsertion.live'] },
  { slug: 'PRIMERX', name: 'PrimeRx', senderDomains: ['guest-poster.com', 'primerx.guestpost-now.com'] },
  { slug: 'SMOKEA', name: 'SMOKEA', senderDomains: ['guest-post.live', 'smokea.guest-post.live'] },
  { slug: 'MRSKIN', name: 'MrSkin', senderDomains: ['ecom-links.com', 'mrskin.ecom-links.com'] },
  { slug: 'VRAI', name: 'VRAI', senderDomains: ['linkinsertion.us', 'vrai.linkinsertion.us'] },
  { slug: 'AMS-FULFILLMENT', name: 'AMS Fulfillment', senderDomains: ['kobo.linkinsertion.org', 'amsfulfillment.ecom-ranker.com'] },
  { slug: 'BUILT-BAR', name: 'Built Bar', senderDomains: ['opensea.linkinsertion.org', 'builtbar.ecom-guestposts.com'] },
  { slug: 'NUTRABIO', name: 'NutraBio', senderDomains: ['linkinsertion.org', 'nutrabio.guest-poster.com'] },
  { slug: 'VIVANTE-LIVING', name: 'Vivante Living', senderDomains: ['ecom-ranker.com', 'vivante.linkinsertion.live'] },
  { slug: 'GOODR', name: 'Goodr', senderDomains: ['ecom-guestposts.com', 'goodr.guestpost-now.com'] },
];

// ── Cron Schedule Expectations ──────────────────────────────────────────────

const CRON_SCHEDULE = {
  'gmail-poll':         { maxAgeMinutes: 10,   frequency: 'every 2 min' },
  'sender-health':      { maxAgeMinutes: 1500, frequency: 'daily' },       // ~25 hours
  'bounce-cleanup':     { maxAgeMinutes: 1500, frequency: 'daily' },
  'prospect-replenish': { maxAgeMinutes: 10500, frequency: 'weekly' },     // ~7.3 days
  'self-improve':       { maxAgeMinutes: 10500, frequency: 'weekly' },
  'weekly-digest':      { maxAgeMinutes: 10500, frequency: 'weekly' },
};

// ── Thresholds ──────────────────────────────────────────────────────────────

const THRESHOLDS = {
  prospectMinDays: 3,          // Alert if < 3 days of inventory
  dailySendRate: 55,           // Expected emails/day per client
  replyGapHours: 4,            // Alert if no replies processed in 4+ hours (business hours)
  negotiationStallHours: 48,   // Alert if negotiation stuck > 48 hours
  openRateDropPct: 20,         // Alert if open rate drops > 20% vs 7-day avg
  replyRateChangePct: 30,      // Alert if reply rate changes > 30%
  bounceRateSpikeMultiplier: 2, // Alert if bounce rate > 2x normal
  negotiationWinDropPct: 15,   // Alert if win rate drops > 15%
  zeroDaysThreshold: 3,        // Alert if 0 replies in 3+ days
  costPerLinkMax: 120,         // Flag if cost/link > $120
  replyRateMin: 0.02,          // Flag if reply rate < 2%
  reducedSendRate: 30,         // Reduced volume when issues detected
};

// ── Airtable Helpers ────────────────────────────────────────────────────────

function getBase() {
  const Airtable = require('airtable');
  Airtable.configure({ apiKey: AIRTABLE_PAT });
  // Trim env var in case of trailing whitespace/newlines
  return Airtable.base((AIRTABLE_BASE || '').trim());
}

async function airtableSelect(table, opts = {}) {
  const base = getBase();
  try {
    const records = await base(table).select(opts).all();
    return records.map(r => ({ id: r.id, ...r.fields }));
  } catch (err) {
    console.error(`[manager] Airtable select from ${table} failed:`, err.message);
    return [];
  }
}

async function airtableCreate(table, fields) {
  const base = getBase();
  try {
    const record = await base(table).create(fields);
    return { id: record.id, ...record.fields };
  } catch (err) {
    console.error(`[manager] Airtable create in ${table} failed:`, err.message);
    return null;
  }
}

// ── Claude API ──────────────────────────────────────────────────────────────

async function callClaude(system, user) {
  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'x-api-key': ANTHROPIC_KEY,
      'anthropic-version': '2023-06-01',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 800,
      system,
      messages: [{ role: 'user', content: user }],
    }),
  });
  const data = await res.json();
  return data.content?.[0]?.text || '';
}

// ── Brevo Helpers ───────────────────────────────────────────────────────────

async function getRecentCampaigns(startDate, endDate) {
  const campaigns = [];
  let offset = 0;
  const limit = 50;
  let hasMore = true;

  // Brevo requires full ISO datetime format for campaign date filters
  // End date must not exceed current time — Brevo rejects future end dates
  const startISO = startDate.includes('T') ? startDate : `${startDate}T00:00:00.000Z`;
  const nowISO = new Date().toISOString();
  const candidateEnd = endDate.includes('T') ? endDate : `${endDate}T23:59:59.999Z`;
  const endISO = candidateEnd > nowISO ? nowISO : candidateEnd;

  while (hasMore) {
    const data = await brevoFetch(
      `/emailCampaigns?status=sent&limit=${limit}&offset=${offset}&startDate=${startISO}&endDate=${endISO}`
    );
    const batch = data.campaigns || [];
    campaigns.push(...batch);
    hasMore = batch.length === limit;
    offset += limit;
  }

  return campaigns;
}

function campaignsByClient(campaigns) {
  const map = {};
  for (const client of CLIENTS) {
    map[client.slug] = [];
  }

  for (const c of campaigns) {
    const senderEmail = c.sender?.email || '';
    const senderDomain = senderEmail.split('@')[1]?.toLowerCase() || '';
    for (const client of CLIENTS) {
      if (client.senderDomains.some(d => senderDomain === d || senderDomain.endsWith('.' + d))) {
        map[client.slug].push(c);
        break;
      }
    }
  }

  return map;
}

function aggregateStats(campaigns) {
  let sent = 0, delivered = 0, opens = 0, clicks = 0, replies = 0;
  let hardBounces = 0, softBounces = 0, complaints = 0;

  for (const c of campaigns) {
    const s = c.statistics?.globalStats || c.statistics?.campaignStats?.[0] || {};
    sent += s.sent || 0;
    delivered += s.delivered || 0;
    opens += s.uniqueOpens || 0;
    clicks += s.uniqueClicks || 0;
    replies += s.replied || s.uniqueReplies || 0;
    hardBounces += s.hardBounces || 0;
    softBounces += s.softBounces || 0;
    complaints += s.complaints || s.spamReports || 0;
  }

  return {
    sent, delivered, opens, clicks, replies,
    hardBounces, softBounces, complaints,
    openRate: delivered > 0 ? opens / delivered : 0,
    replyRate: delivered > 0 ? replies / delivered : 0,
    bounceRate: sent > 0 ? (hardBounces + softBounces) / sent : 0,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 1: SYSTEM HEALTH CHECK
// ─────────────────────────────────────────────────────────────────────────────

async function checkSystemHealth() {
  console.log('[manager] Checking system health...');
  const results = [];

  // Read last_run timestamps from SystemHealth table
  const healthRecords = await airtableSelect('SystemHealth', {
    filterByFormula: '{Type} = "cron_run"',
    sort: [{ field: 'Date', direction: 'desc' }],
  });

  const now = Date.now();

  for (const [cronName, schedule] of Object.entries(CRON_SCHEDULE)) {
    // Find most recent record for this cron
    const lastRun = healthRecords.find(r => r.CronName === cronName);
    const lastRunTime = lastRun ? new Date(lastRun.Date || lastRun.LastRun).getTime() : 0;
    const ageMinutes = lastRunTime > 0 ? (now - lastRunTime) / 60000 : Infinity;

    let status = 'ok';
    let detail = '';

    if (lastRunTime === 0) {
      status = 'unknown';
      detail = 'No run recorded';
    } else if (ageMinutes > schedule.maxAgeMinutes) {
      status = 'overdue';
      detail = `Last ran ${formatAge(ageMinutes)} ago (expected ${schedule.frequency})`;
    } else {
      detail = `Last ran ${formatAge(ageMinutes)} ago`;
    }

    results.push({ cronName, status, detail, lastRunTime, ageMinutes });
  }

  return results;
}

function formatAge(minutes) {
  if (minutes < 60) return `${Math.round(minutes)} min`;
  if (minutes < 1440) return `${Math.round(minutes / 60)} hours`;
  return `${Math.round(minutes / 1440)} days`;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 2: PIPELINE INTEGRITY
// ─────────────────────────────────────────────────────────────────────────────

async function checkPipelineIntegrity() {
  console.log('[manager] Checking pipeline integrity...');
  const alerts = [];

  // 2a. Prospect inventory per client — check queued Brevo campaigns per client
  let queuedCampaigns = [];
  try {
    const data = await brevoFetch('/emailCampaigns?status=queued&limit=200&offset=0');
    queuedCampaigns = data.campaigns || [];
    console.log(`[manager] Brevo queued campaigns: ${queuedCampaigns.length}`);
  } catch (err) {
    console.error('[manager] Failed to fetch queued campaigns:', err.message);
    alerts.push({
      level: 'critical',
      client: 'SYSTEM',
      message: `Brevo API error: ${err.message?.slice(0, 100)}`,
    });
  }

  const queuedByClient = {};
  for (const client of CLIENTS) {
    queuedByClient[client.slug] = 0;
  }
  for (const c of queuedCampaigns) {
    const senderEmail = c.sender?.email || '';
    const senderDomain = senderEmail.split('@')[1]?.toLowerCase() || '';
    for (const client of CLIENTS) {
      if (client.senderDomains.some(d => senderDomain === d || senderDomain.endsWith('.' + d))) {
        queuedByClient[client.slug]++;
        break;
      }
    }
  }

  for (const client of CLIENTS) {
    const queued = queuedByClient[client.slug] || 0;
    if (queued === 0) {
      alerts.push({
        level: 'warning',
        client: client.name,
        message: `0 queued campaigns — no scheduled emails pending`,
        autoFixable: 'replenish',
      });
    } else {
      // Each campaign typically sends ~55 emails, so queued campaigns ≈ days of inventory
      const approxDays = queued; // ~1 campaign per day per client
      if (approxDays < THRESHOLDS.prospectMinDays) {
        alerts.push({
          level: 'warning',
          client: client.name,
          message: `${queued} queued campaigns (< ${THRESHOLDS.prospectMinDays} days of inventory)`,
          autoFixable: 'replenish',
        });
      }
    }
  }

  // 2b. Stalled negotiations (> 48 hours without response)
  const negotiations = await airtableSelect('Negotiations', {
    filterByFormula: `AND(
      {Action} != "closed",
      {Action} != "won",
      {Action} != "lost",
      {Action} != "rejected",
      {Direction} = "inbound"
    )`,
    sort: [{ field: 'Date', direction: 'desc' }],
  });

  const now = Date.now();
  const seenDomains = new Set();

  for (const neg of negotiations) {
    const domain = neg.Domain || '';
    if (seenDomains.has(domain)) continue;
    seenDomains.add(domain);

    const lastDate = new Date(neg.Date).getTime();
    const hoursStalled = (now - lastDate) / 3600000;

    if (hoursStalled > THRESHOLDS.negotiationStallHours) {
      alerts.push({
        level: 'warning',
        client: neg.Client || 'Unknown',
        message: `Negotiation with ${domain}: stalled ${Math.round(hoursStalled)} hours`,
      });
    }
  }

  // 2c. Reply processing check — has gmail-poll been active?
  const recentHealth = await airtableSelect('SystemHealth', {
    filterByFormula: `AND({CronName} = "gmail-poll", {Type} = "cron_run")`,
    sort: [{ field: 'Date', direction: 'desc' }],
    maxRecords: 1,
  });

  if (recentHealth.length > 0) {
    const lastPoll = new Date(recentHealth[0].Date || recentHealth[0].LastRun).getTime();
    const hoursSincePoll = (now - lastPoll) / 3600000;
    const currentHourSAST = (new Date().getUTCHours() + 2) % 24;
    const isBusinessHours = currentHourSAST >= 8 && currentHourSAST <= 18;

    if (isBusinessHours && hoursSincePoll > THRESHOLDS.replyGapHours) {
      alerts.push({
        level: 'warning',
        client: 'ALL',
        message: `gmail-poll inactive for ${Math.round(hoursSincePoll)} hours during business hours`,
      });
    }
  }

  // 2d. Drip cancellation integrity — check for replies where drips may not have been cancelled
  // Look for recent inbound negotiations and verify their drip campaigns were suspended
  const recentInbound = await airtableSelect('Negotiations', {
    filterByFormula: `AND(
      {Direction} = "inbound",
      IS_AFTER({Date}, DATEADD(NOW(), -2, 'days'))
    )`,
    sort: [{ field: 'Date', direction: 'desc' }],
    maxRecords: 50,
  });

  for (const neg of recentInbound) {
    const domain = neg.Domain || '';
    // Check if there's an outreach record with campaign IDs
    const outreach = await airtableSelect('Outreach', {
      filterByFormula: `LOWER({Domain}) = "${domain.toLowerCase()}"`,
      maxRecords: 1,
    });

    if (outreach.length > 0 && outreach[0].CampaignIds) {
      // Parse campaign IDs and check if any are still scheduled
      let campaignIds;
      try {
        campaignIds = JSON.parse(outreach[0].CampaignIds);
      } catch {
        campaignIds = [];
      }

      for (const cid of campaignIds) {
        try {
          const campaign = await brevoFetch(`/emailCampaigns/${cid}`);
          if (campaign.status === 'queued' || campaign.status === 'draft') {
            alerts.push({
              level: 'critical',
              client: neg.Client || 'Unknown',
              message: `Drip campaign #${cid} still scheduled for ${domain} AFTER they replied — needs cancellation`,
              autoFixable: 'cancel_drip',
              meta: { campaignId: cid },
            });
          }
        } catch {
          // Campaign may not exist or API error — skip
        }
      }
    }
  }

  return alerts;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 3: PERFORMANCE ANOMALY DETECTION
// ─────────────────────────────────────────────────────────────────────────────

async function detectAnomalies() {
  console.log('[manager] Detecting performance anomalies...');
  const anomalies = [];

  const now = new Date();
  const todayStr = now.toISOString().split('T')[0];
  const sevenDaysAgo = new Date(now - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
  const fourteenDaysAgo = new Date(now - 14 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

  // Get campaigns for the last 7 days and the 7 days before that
  const [recentCampaigns, priorCampaigns] = await Promise.all([
    getRecentCampaigns(sevenDaysAgo, todayStr),
    getRecentCampaigns(fourteenDaysAgo, sevenDaysAgo),
  ]);

  const recentByClient = campaignsByClient(recentCampaigns);
  const priorByClient = campaignsByClient(priorCampaigns);

  // Also get negotiation data for win rate analysis
  const recentNegotiations = await airtableSelect('Negotiations', {
    filterByFormula: `IS_AFTER({Date}, DATEADD(NOW(), -7, 'days'))`,
  });
  const priorNegotiations = await airtableSelect('Negotiations', {
    filterByFormula: `AND(
      IS_AFTER({Date}, DATEADD(NOW(), -14, 'days')),
      IS_BEFORE({Date}, DATEADD(NOW(), -7, 'days'))
    )`,
  });

  for (const client of CLIENTS) {
    const recent = aggregateStats(recentByClient[client.slug] || []);
    const prior = aggregateStats(priorByClient[client.slug] || []);

    // Skip clients with no recent data
    if (recent.sent === 0) continue;

    // CRITICAL: 0% open rate with significant volume = deliverability failure
    if (recent.opens === 0 && recent.sent >= 20) {
      anomalies.push({
        level: 'critical',
        client: client.name,
        metric: 'zero_opens',
        message: `0% open rate on ${recent.sent} emails — ALL emails likely going to spam. Pause campaigns and check DNS/DKIM/SPF for sender domains.`,
        autoFixable: 'pause_campaigns',
      });
    } else if (recent.openRate > 0 && recent.openRate < 0.03 && recent.sent >= 20) {
      // Open rate below 3% = severe deliverability issue
      anomalies.push({
        level: 'critical',
        client: client.name,
        metric: 'open_rate',
        message: `Open rate ${(recent.openRate * 100).toFixed(1)}% on ${recent.sent} emails — severe deliverability issue. Check sender reputation.`,
        autoFixable: 'reduce_volume',
      });
    } else if (prior.openRate > 0 && recent.openRate > 0) {
      // Open rate drop > 20% vs prior period
      const dropPct = ((prior.openRate - recent.openRate) / prior.openRate) * 100;
      if (dropPct > THRESHOLDS.openRateDropPct) {
        anomalies.push({
          level: 'warning',
          client: client.name,
          metric: 'open_rate',
          message: `Open rate dropped ${Math.round(dropPct)}% vs 7-day avg (${(recent.openRate * 100).toFixed(1)}% → possible deliverability issue)`,
          autoFixable: 'reduce_volume',
        });
      }
    }

    // Reply rate change > 30%
    if (prior.replyRate > 0 && recent.replyRate > 0) {
      const changePct = ((recent.replyRate - prior.replyRate) / prior.replyRate) * 100;
      if (Math.abs(changePct) > THRESHOLDS.replyRateChangePct) {
        const direction = changePct > 0 ? 'spiked' : 'dropped';
        anomalies.push({
          level: changePct < 0 ? 'warning' : 'info',
          client: client.name,
          metric: 'reply_rate',
          message: `Reply rate ${direction} ${Math.abs(Math.round(changePct))}% vs prior week (${(recent.replyRate * 100).toFixed(1)}%)`,
        });
      }
    }

    // Bounce rate spike > 2x
    if (prior.bounceRate > 0 && recent.bounceRate > prior.bounceRate * THRESHOLDS.bounceRateSpikeMultiplier) {
      anomalies.push({
        level: 'critical',
        client: client.name,
        metric: 'bounce_rate',
        message: `Bounce rate ${(recent.bounceRate * 100).toFixed(1)}% — ${(recent.bounceRate / prior.bounceRate).toFixed(1)}x higher than prior week`,
        autoFixable: 'reduce_volume',
      });
    }

    // 0 replies in 3+ days
    if (recent.replies === 0 && recent.sent > THRESHOLDS.dailySendRate * THRESHOLDS.zeroDaysThreshold) {
      anomalies.push({
        level: 'warning',
        client: client.name,
        metric: 'zero_replies',
        message: `0 replies from ${recent.sent} emails sent in the last 7 days`,
      });
    }
  }

  // Negotiation win rate analysis
  const recentWins = recentNegotiations.filter(n => n.Action === 'won').length;
  const recentTotal = recentNegotiations.filter(n => ['won', 'lost', 'rejected'].includes(n.Action)).length;
  const priorWins = priorNegotiations.filter(n => n.Action === 'won').length;
  const priorTotal = priorNegotiations.filter(n => ['won', 'lost', 'rejected'].includes(n.Action)).length;

  if (recentTotal >= 5 && priorTotal >= 5) {
    const recentWinRate = recentWins / recentTotal;
    const priorWinRate = priorWins / priorTotal;
    const dropPct = priorWinRate > 0 ? ((priorWinRate - recentWinRate) / priorWinRate) * 100 : 0;

    if (dropPct > THRESHOLDS.negotiationWinDropPct) {
      anomalies.push({
        level: 'warning',
        client: 'ALL',
        metric: 'negotiation_win_rate',
        message: `Negotiation win rate dropped ${Math.round(dropPct)}% (${(recentWinRate * 100).toFixed(0)}% vs ${(priorWinRate * 100).toFixed(0)}% prior week)`,
      });
    }
  }

  return anomalies;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 4: COST EFFICIENCY ANALYSIS
// ─────────────────────────────────────────────────────────────────────────────

async function analyzeCostEfficiency(recentCampaignsByClient) {
  console.log('[manager] Analyzing cost efficiency...');
  const analysis = [];

  // Get financial data from Airtable
  const thisMonth = new Date().toISOString().slice(0, 7); // YYYY-MM
  const finances = await airtableSelect('Finances', {
    filterByFormula: `DATETIME_FORMAT({Date}, 'YYYY-MM') = "${thisMonth}"`,
  });

  // Get links placed this month
  const links = await airtableSelect('Links', {
    filterByFormula: `DATETIME_FORMAT({Date Placed}, 'YYYY-MM') = "${thisMonth}"`,
  });

  for (const client of CLIENTS) {
    const clientFinances = finances.filter(f => f.Client === client.name || f.Client === client.slug);
    const clientLinks = links.filter(l => l.Client === client.name || l.Client === client.slug);

    const totalCost = clientFinances
      .filter(f => f.Type === 'Cost')
      .reduce((sum, f) => sum + (f.Amount || 0), 0);
    const totalRevenue = clientFinances
      .filter(f => f.Type === 'Revenue')
      .reduce((sum, f) => sum + (f.Amount || 0), 0);
    const linksPlaced = clientLinks.length;

    const costPerLink = linksPlaced > 0 ? totalCost / linksPlaced : null;

    // Get reply rate from recent campaigns
    const clientCampaigns = recentCampaignsByClient[client.slug] || [];
    const stats = aggregateStats(clientCampaigns);

    const entry = {
      client: client.name,
      slug: client.slug,
      linksPlaced,
      totalCost: Math.round(totalCost),
      totalRevenue: Math.round(totalRevenue),
      costPerLink: costPerLink !== null ? Math.round(costPerLink) : null,
      replyRate: stats.replyRate,
      sent: stats.sent,
      openRate: stats.openRate,
      alerts: [],
    };

    if (costPerLink !== null && costPerLink > THRESHOLDS.costPerLinkMax) {
      entry.alerts.push(`Cost/link $${Math.round(costPerLink)} > $${THRESHOLDS.costPerLinkMax} threshold — eroding margins`);
    }
    if (stats.replyRate > 0 && stats.replyRate < THRESHOLDS.replyRateMin) {
      entry.alerts.push(`Reply rate ${(stats.replyRate * 100).toFixed(1)}% < ${THRESHOLDS.replyRateMin * 100}% — may need different approach`);
    }

    analysis.push(entry);
  }

  return analysis;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 5: AUTO-FIX CAPABILITIES
// ─────────────────────────────────────────────────────────────────────────────

async function executeAutoFixes(pipelineAlerts, anomalies) {
  console.log('[manager] Evaluating auto-fix actions...');
  const actions = [];

  // 5a. Cancel orphaned drip campaigns
  const dripAlerts = pipelineAlerts.filter(a => a.autoFixable === 'cancel_drip');
  for (const alert of dripAlerts) {
    try {
      await brevoFetch(`/emailCampaigns/${alert.meta.campaignId}`, {
        method: 'PUT',
        body: { status: 'suspended' },
      });
      actions.push({
        action: 'Cancelled orphaned drip campaign',
        client: alert.client,
        detail: `Campaign #${alert.meta.campaignId} suspended`,
        automated: true,
      });
    } catch (err) {
      console.warn(`[manager] Failed to cancel drip ${alert.meta.campaignId}:`, err.message);
    }
  }

  // 5b. Reduce send volume for clients with deliverability issues
  const volumeAlerts = anomalies.filter(a => a.autoFixable === 'reduce_volume');
  for (const alert of volumeAlerts) {
    // Log the volume reduction in Airtable — actual volume adjustment is handled
    // by push-to-brevo checking the SendVolumeOverride field
    const client = CLIENTS.find(c => c.name === alert.client);
    if (client) {
      await airtableCreate('ManagerActions', {
        Date: new Date().toISOString(),
        Action: 'reduce_volume',
        Client: client.name,
        Reason: alert.message,
        Detail: `Reduced daily send volume from ${THRESHOLDS.dailySendRate} to ${THRESHOLDS.reducedSendRate}`,
        Automated: true,
      });

      // Set volume override in SystemHealth so push-to-brevo respects it
      await airtableCreate('SystemHealth', {
        Type: 'volume_override',
        CronName: 'manager',
        Client: client.slug,
        Value: String(THRESHOLDS.reducedSendRate),
        Date: new Date().toISOString(),
        Reason: alert.message,
      });

      actions.push({
        action: `Reduced ${client.name} send volume to ${THRESHOLDS.reducedSendRate}/day`,
        client: client.name,
        detail: alert.message,
        automated: true,
      });
    }
  }

  // 5c. Emergency prospect replenishment — trigger if ANY client has 0 queued campaigns
  const replenishAlerts = pipelineAlerts.filter(
    a => a.autoFixable === 'replenish' && a.message.includes('0 queued')
  );
  if (replenishAlerts.length > 0) {
    // Actually trigger the prospect-replenish cron endpoint
    const baseUrl = process.env.VERCEL_URL
      ? `https://${process.env.VERCEL_URL}`
      : 'https://pror-engine.vercel.app';
    try {
      const headers = {};
      if (process.env.CRON_SECRET) {
        headers.Authorization = `Bearer ${process.env.CRON_SECRET}`;
      }
      await fetch(`${baseUrl}/api/cron/prospect-replenish`, { method: 'GET', headers });
      console.log('[manager] Emergency replenish triggered via HTTP');
    } catch (err) {
      console.error('[manager] Emergency replenish call failed:', err.message);
    }

    // Log the action
    await airtableCreate('ManagerActions', {
      Date: new Date().toISOString(),
      Action: 'emergency_replenish',
      Client: replenishAlerts.map(a => a.client).join(', '),
      Reason: replenishAlerts.map(a => `${a.client}: ${a.message}`).join('; '),
      Detail: 'Emergency replenishment triggered via HTTP call to prospect-replenish',
      Automated: true,
    });

    actions.push({
      action: `Triggered emergency replenish for ${replenishAlerts.map(a => a.client).join(', ')}`,
      client: replenishAlerts.map(a => a.client).join(', '),
      detail: 'Called /api/cron/prospect-replenish endpoint',
      automated: true,
    });
  }

  // 5d. Pause sender domains with critical bounce rates
  const criticalBounce = anomalies.filter(a => a.level === 'critical' && a.metric === 'bounce_rate');
  for (const alert of criticalBounce) {
    const client = CLIENTS.find(c => c.name === alert.client);
    if (client) {
      // Pause scheduled campaigns for this client's sender domains
      let pausedCount = 0;
      try {
        const queuedData = await brevoFetch('/emailCampaigns?status=queued&limit=200&offset=0');
        const queued = queuedData.campaigns || [];

        for (const campaign of queued) {
          const senderEmail = campaign.sender?.email || '';
          const senderDomain = senderEmail.split('@')[1]?.toLowerCase() || '';
          if (client.senderDomains.includes(senderDomain)) {
            try {
              await brevoFetch(`/emailCampaigns/${campaign.id}`, {
                method: 'PUT',
                body: { status: 'suspended' },
              });
              pausedCount++;
            } catch {
              // Some campaigns may not be suspendable
            }
          }
        }
      } catch (err) {
        console.warn(`[manager] Error pausing campaigns for ${client.name}:`, err.message);
      }

      if (pausedCount > 0) {
        await airtableCreate('ManagerActions', {
          Date: new Date().toISOString(),
          Action: 'pause_sender',
          Client: client.name,
          Reason: alert.message,
          Detail: `Paused ${pausedCount} scheduled campaigns`,
          Automated: true,
        });

        actions.push({
          action: `Paused ${pausedCount} campaigns for ${client.name}`,
          client: client.name,
          detail: `Critical bounce rate — ${alert.message}`,
          automated: true,
        });
      }
    }
  }

  // 5e. Auto-pause campaigns for clients with 0% open rate (deliverability failure)
  const zeroOpenAlerts = anomalies.filter(a => a.autoFixable === 'pause_campaigns' && a.metric === 'zero_opens');
  for (const alert of zeroOpenAlerts) {
    const client = CLIENTS.find(c => c.name === alert.client);
    if (client) {
      let pausedCount = 0;
      try {
        const queuedData = await brevoFetch('/emailCampaigns?status=queued&limit=200&offset=0');
        const queued = queuedData.campaigns || [];

        for (const campaign of queued) {
          const senderEmail = campaign.sender?.email || '';
          const senderDomain = senderEmail.split('@')[1]?.toLowerCase() || '';
          if (client.senderDomains.some(d => senderDomain === d || senderDomain.endsWith('.' + d))) {
            try {
              await brevoFetch(`/emailCampaigns/${campaign.id}`, {
                method: 'PUT',
                body: { status: 'suspended' },
              });
              pausedCount++;
            } catch {
              // Some campaigns may not be suspendable
            }
          }
        }
      } catch (err) {
        console.warn(`[manager] Error pausing campaigns for ${client.name}:`, err.message);
      }

      if (pausedCount > 0) {
        await airtableCreate('ManagerActions', {
          Date: new Date().toISOString(),
          Action: 'pause_zero_opens',
          Client: client.name,
          Reason: `0% open rate — all emails going to spam`,
          Detail: `Paused ${pausedCount} campaigns. Check DNS/DKIM/SPF for ${client.senderDomains.join(', ')}`,
          Automated: true,
        });

        actions.push({
          action: `⛔ PAUSED ${pausedCount} campaigns for ${client.name} — 0% open rate, emails going to spam. Check DKIM/SPF for: ${client.senderDomains.join(', ')}`,
          client: client.name,
          detail: 'Deliverability failure — campaigns auto-paused until DNS is fixed',
          automated: true,
        });
      }
    }
  }

  return actions;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 6: IMPROVEMENT SUGGESTIONS (Claude Haiku)
// ─────────────────────────────────────────────────────────────────────────────

async function generateSuggestions(costAnalysis, anomalies) {
  console.log('[manager] Generating improvement suggestions...');

  // Build metrics summary for Claude
  const metricsTable = costAnalysis.map(c => {
    return `${c.client}: ${c.sent} sent, ${(c.openRate * 100).toFixed(1)}% open, ${(c.replyRate * 100).toFixed(1)}% reply, ${c.linksPlaced} links, $${c.costPerLink || 'N/A'}/link, $${c.totalRevenue} rev, $${c.totalCost} cost`;
  }).join('\n');

  const anomalySummary = anomalies.length > 0
    ? `\nAnomalies detected:\n${anomalies.map(a => `- ${a.client}: ${a.message}`).join('\n')}`
    : '\nNo anomalies detected.';

  const system = `You are a senior link-building operations analyst. You help optimize a 13-client outreach machine that sends ~55 emails/day per client. Be specific — name the client, the metric, and exactly what to change. Each suggestion must be one sentence. Never suggest anything generic. Focus on actionable, data-driven improvements.`;

  const user = `Here are the current metrics for 13 link-building outreach clients (this month):\n\n${metricsTable}\n${anomalySummary}\n\nIdentify the top 3 actionable improvements. Be specific — name the client, the metric, and exactly what to change. Keep each suggestion to one sentence.`;

  try {
    const suggestions = await callClaude(system, user);
    return suggestions.split('\n').filter(s => s.trim().length > 5);
  } catch (err) {
    console.error('[manager] Claude suggestions failed:', err.message);
    return ['(Suggestions unavailable — Claude API error)'];
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 7: SLACK REPORT
// ─────────────────────────────────────────────────────────────────────────────

async function postSlackReport(health, pipelineAlerts, anomalies, costAnalysis, autoActions, suggestions, quickStats, deliverability = {}) {
  const now = new Date();
  const timeSAST = now.toLocaleTimeString('en-US', {
    timeZone: 'Africa/Johannesburg',
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
  });
  const dateSAST = now.toLocaleDateString('en-US', {
    timeZone: 'Africa/Johannesburg',
    month: 'long',
    day: 'numeric',
  });

  let msg = `\ud83e\udd16 *Manager Check-In — ${dateSAST}, ${timeSAST}*\n\n`;

  // ── System Status ──
  msg += `\u2501\u2501\u2501 SYSTEM STATUS \u2501\u2501\u2501\n`;
  for (const cron of health) {
    const emoji = cron.status === 'ok' ? '\u2705' : cron.status === 'overdue' ? '\u26a0\ufe0f' : '\u2753';
    msg += `${emoji} ${cron.cronName}: ${cron.detail}\n`;
  }
  msg += '\n';

  // ── Deliverability Health ──
  if (deliverability.totalSent > 0) {
    const openPct = (deliverability.overallOpenRate * 100).toFixed(1);
    const replyPct = (deliverability.overallReplyRate * 100).toFixed(1);
    const bouncePct = (deliverability.overallBounceRate * 100).toFixed(1);
    const openEmoji = deliverability.overallOpenRate === 0 ? '\ud83d\udea8' : deliverability.overallOpenRate < 0.1 ? '\u26a0\ufe0f' : '\u2705';
    const pollEmoji = deliverability.gmailPollHealthy ? '\u2705' : '\ud83d\udea8';

    msg += `\u2501\u2501\u2501 DELIVERABILITY (7-day) \u2501\u2501\u2501\n`;
    msg += `${openEmoji} Open rate: ${openPct}% | Replies: ${deliverability.totalReplies} | Bounce: ${bouncePct}%\n`;
    msg += `\ud83d\udce7 Sent: ${deliverability.totalSent} | Delivered: ${deliverability.totalDelivered}\n`;
    msg += `${pollEmoji} Gmail-poll: ${deliverability.gmailPollLastRun !== null ? `last ran ${Math.round(deliverability.gmailPollLastRun)} min ago` : 'never logged — may be failing silently'}\n`;
    if (deliverability.overallOpenRate === 0) {
      msg += `\ud83d\udea8 *CRITICAL: 0% open rate = all emails going to spam. Check DKIM/SPF/DMARC on all sender domains.*\n`;
    }
    msg += '\n';
  }

  // ── Pipeline Alerts ──
  if (pipelineAlerts.length > 0) {
    msg += `\u2501\u2501\u2501 PIPELINE ALERTS \u2501\u2501\u2501\n`;
    for (const alert of pipelineAlerts) {
      const emoji = alert.level === 'critical' ? '\ud83d\udd34' : '\u26a0\ufe0f';
      msg += `${emoji} ${alert.client}: ${alert.message}\n`;
    }
    msg += '\n';
  }

  // ── Anomalies ──
  msg += `\u2501\u2501\u2501 ANOMALIES \u2501\u2501\u2501\n`;
  const significantAnomalies = anomalies.filter(a => a.level !== 'info');
  if (significantAnomalies.length > 0) {
    for (const anomaly of significantAnomalies) {
      const emoji = anomaly.level === 'critical' ? '\ud83d\udd34' : '\ud83d\udcc9';
      msg += `${emoji} ${anomaly.client}: ${anomaly.message}\n`;
    }
  } else {
    msg += `\ud83d\udcca All clients within normal range\n`;
  }
  msg += '\n';

  // ── Auto-Actions Taken ──
  if (autoActions.length > 0) {
    msg += `\u2501\u2501\u2501 AUTO-ACTIONS TAKEN \u2501\u2501\u2501\n`;
    for (const action of autoActions) {
      msg += `\ud83d\udd27 ${action.action}\n`;
    }
    msg += '\n';
  }

  // ── Cost Alerts ──
  const costAlerts = costAnalysis.filter(c => c.alerts.length > 0);
  if (costAlerts.length > 0) {
    msg += `\u2501\u2501\u2501 COST ALERTS \u2501\u2501\u2501\n`;
    for (const entry of costAlerts) {
      for (const alert of entry.alerts) {
        msg += `\ud83d\udcb0 ${entry.client}: ${alert}\n`;
      }
    }
    msg += '\n';
  }

  // ── Suggestions ──
  if (suggestions.length > 0) {
    msg += `\u2501\u2501\u2501 \ud83d\udca1 SUGGESTIONS \u2501\u2501\u2501\n`;
    for (const suggestion of suggestions) {
      const cleaned = suggestion.replace(/^\d+[\.\)]\s*/, '').replace(/^[-•]\s*/, '');
      if (cleaned.length > 5) {
        msg += `\u2022 ${cleaned}\n`;
      }
    }
    msg += '\n';
  }

  // ── Quick Stats ──
  msg += `\u2501\u2501\u2501 \ud83d\udcca QUICK STATS \u2501\u2501\u2501\n`;
  msg += `Active clients: ${quickStats.activeClients} | `;
  msg += `Emails today: ${quickStats.emailsToday} | `;
  msg += `Replies today: ${quickStats.repliesToday}\n`;
  msg += `Open negotiations: ${quickStats.openNegotiations} | `;
  msg += `Links this month: ${quickStats.linksThisMonth} | `;
  msg += `Revenue this month: $${quickStats.revenueThisMonth}`;

  // Determine if we need to @channel for critical issues
  const hasCritical = pipelineAlerts.some(a => a.level === 'critical') ||
                      anomalies.some(a => a.level === 'critical');
  if (hasCritical) {
    msg = `<!channel> ${msg}`;
  }

  await slack.post(CHANNEL(), msg);
  console.log('[manager] Slack report posted');
}

// ─────────────────────────────────────────────────────────────────────────────
// QUICK STATS GATHERER
// ─────────────────────────────────────────────────────────────────────────────

async function gatherQuickStats() {
  const todayStr = new Date().toISOString().split('T')[0];
  const thisMonth = todayStr.slice(0, 7);

  // Get today's campaign stats
  const todayCampaigns = await getRecentCampaigns(todayStr, todayStr);
  const todayStats = aggregateStats(todayCampaigns);

  // Get open negotiations
  const openNeg = await airtableSelect('Negotiations', {
    filterByFormula: `AND(
      {Action} != "closed",
      {Action} != "won",
      {Action} != "lost",
      {Action} != "rejected"
    )`,
  });

  // Dedupe by domain for open negotiations count
  const uniqueNegDomains = new Set(openNeg.map(n => n.Domain).filter(Boolean));

  // Get links this month
  const monthLinks = await airtableSelect('Links', {
    filterByFormula: `DATETIME_FORMAT({Date Placed}, 'YYYY-MM') = "${thisMonth}"`,
  });

  // Get revenue this month
  const monthFinances = await airtableSelect('Finances', {
    filterByFormula: `AND(
      DATETIME_FORMAT({Date}, 'YYYY-MM') = "${thisMonth}",
      {Type} = "Revenue"
    )`,
  });
  const revenue = monthFinances.reduce((sum, f) => sum + (f.Amount || 0), 0);

  return {
    activeClients: CLIENTS.length,
    emailsToday: todayStats.sent,
    repliesToday: todayStats.replies,
    openNegotiations: uniqueNegDomains.size,
    linksThisMonth: monthLinks.length,
    revenueThisMonth: Math.round(revenue),
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN RUN FUNCTION
// ─────────────────────────────────────────────────────────────────────────────

async function run() {
  console.log('[manager] Starting Manager check-in...');
  const startTime = Date.now();

  try {
    // 1. System health check
    const health = await checkSystemHealth();

    // 2. Pipeline integrity
    const pipelineAlerts = await checkPipelineIntegrity();

    // 3. Performance anomaly detection (also gets campaign data we reuse)
    const anomalies = await detectAnomalies();

    // 4. Get campaign data for cost analysis
    const todayStr = new Date().toISOString().split('T')[0];
    const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    const recentCampaigns = await getRecentCampaigns(sevenDaysAgo, todayStr);
    const recentByClient = campaignsByClient(recentCampaigns);

    // 5. Deliverability health — aggregate open/reply stats across all clients
    const allRecentStats = aggregateStats(recentCampaigns);
    const deliverability = {
      totalSent: allRecentStats.sent,
      totalDelivered: allRecentStats.delivered,
      totalOpens: allRecentStats.opens,
      totalReplies: allRecentStats.replies,
      overallOpenRate: allRecentStats.openRate,
      overallReplyRate: allRecentStats.replyRate,
      overallBounceRate: allRecentStats.bounceRate,
      gmailPollHealthy: true, // assume healthy until proven otherwise
    };

    // Check gmail-poll health: if SystemHealth has a recent entry, it's running
    const gmailPollRecords = await airtableSelect('SystemHealth', {
      filterByFormula: `AND({Type} = "cron_run", {CronName} = "gmail-poll")`,
      sort: [{ field: 'Date', direction: 'desc' }],
      maxRecords: 1,
    });
    if (gmailPollRecords.length > 0) {
      const lastRun = new Date(gmailPollRecords[0].Date || gmailPollRecords[0].LastRun).getTime();
      const minutesAgo = (Date.now() - lastRun) / 60000;
      deliverability.gmailPollLastRun = minutesAgo;
      deliverability.gmailPollHealthy = minutesAgo < 10; // should run every 2 min
    } else {
      deliverability.gmailPollLastRun = null;
      deliverability.gmailPollHealthy = false; // never logged = probably failing
    }

    // 6. Cost efficiency analysis
    const costAnalysis = await analyzeCostEfficiency(recentByClient);

    // 7. Auto-fix (takes action on critical issues)
    const autoActions = await executeAutoFixes(pipelineAlerts, anomalies);

    // 8. AI suggestions
    const suggestions = await generateSuggestions(costAnalysis, anomalies);

    // 9. Quick stats
    const quickStats = await gatherQuickStats();

    // 10. Post Slack report
    await postSlackReport(health, pipelineAlerts, anomalies, costAnalysis, autoActions, suggestions, quickStats, deliverability);

    // 10. Log this Manager run to SystemHealth
    await airtableCreate('SystemHealth', {
      Type: 'cron_run',
      CronName: 'manager',
      Date: new Date().toISOString(),
      Value: JSON.stringify({
        duration: Date.now() - startTime,
        alerts: pipelineAlerts.length,
        anomalies: anomalies.length,
        autoActions: autoActions.length,
      }),
    });

    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`[manager] Complete in ${duration}s — ${pipelineAlerts.length} alerts, ${anomalies.length} anomalies, ${autoActions.length} auto-actions`);

    return {
      health,
      pipelineAlerts: pipelineAlerts.length,
      anomalies: anomalies.length,
      autoActions: autoActions.length,
      costAlerts: costAnalysis.filter(c => c.alerts.length > 0).length,
      suggestions: suggestions.length,
      durationMs: Date.now() - startTime,
    };
  } catch (err) {
    console.error('[manager] Fatal error:', err);

    // Still try to notify Slack
    try {
      await slack.post(
        CHANNEL(),
        `\ud83d\udea8 *Manager Agent Failed*\n\n\`\`\`${err.message}\`\`\`\n\nCheck Vercel logs for details.`
      );
    } catch (slackErr) {
      console.error('[manager] Could not post error to Slack:', slackErr.message);
    }

    throw err;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// VERCEL SERVERLESS EXPORT
// ─────────────────────────────────────────────────────────────────────────────

module.exports = async (req, res) => {
  waitUntil(run());
  res.status(200).json({ ok: true, message: 'Manager check-in started' });
};
