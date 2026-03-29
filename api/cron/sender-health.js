const { waitUntil } = require('@vercel/functions');
const slack = require('../../shared/slack');

const BREVO_KEY = process.env.BREVO_API_KEY;
const CHANNEL = () => process.env.CHANNEL_COMMAND_CENTER;

// Thresholds
const THRESHOLDS = {
  openRate: { healthy: 0.15, warning: 0.08 },
  bounceRate: { warning: 0.03, critical: 0.05 },
  spamRate: { warning: 0.001, critical: 0.003 },
};

// All sender domains we're monitoring
const SENDER_DOMAINS = [
  'ableammo.ecom-ranker.com',
  'drdabber.ecom-guestposts.com',
  'felina.guest-poster.com',
  'millpkg.linkinsertion.live',
  'primerx.guestpost-now.com',
  'smokea.guest-post.live',
  'mrskin.ecom-links.com',
  'vrai.linkinsertion.us',
  'amsfulfillment.ecom-ranker.com',
  'builtbar.ecom-guestposts.com',
  'nutrabio.guest-poster.com',
  'vivante.linkinsertion.live',
  'goodr.guestpost-now.com',
  // Root domain senders
  'ecom-ranker.com',
  'ecom-guestposts.com',
  'guest-poster.com',
  'linkinsertion.live',
  'guestpost-now.com',
  'guest-post.live',
  'ecom-links.com',
  'linkinsertion.us',
];

async function brevoGet(path) {
  const res = await fetch(`https://api.brevo.com/v3${path}`, {
    headers: { 'api-key': BREVO_KEY, 'Content-Type': 'application/json' },
  });
  return res.json();
}

async function getStatsForDomain(domain) {
  const now = new Date();
  const weekAgo = new Date(now - 7 * 24 * 60 * 60 * 1000);
  const startDate = weekAgo.toISOString().split('T')[0];
  const endDate = now.toISOString().split('T')[0];

  // Pull all sent campaigns and filter by sender domain
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

  // Filter campaigns where sender email matches this domain
  const domainCampaigns = allCampaigns.filter((c) => {
    const senderEmail = c.sender?.email || '';
    return senderEmail.endsWith('@' + domain);
  });

  if (domainCampaigns.length === 0) {
    return null; // No campaigns from this domain in last 7 days
  }

  // Aggregate stats
  let totalSent = 0,
    totalDelivered = 0,
    totalOpens = 0;
  let totalHardBounces = 0,
    totalSoftBounces = 0,
    totalComplaints = 0;

  for (const c of domainCampaigns) {
    const s = c.statistics?.globalStats || c.statistics || {};
    totalSent += s.sent || 0;
    totalDelivered += s.delivered || 0;
    totalOpens += s.uniqueViews || s.uniqueOpens || 0;
    totalHardBounces += s.hardBounces || 0;
    totalSoftBounces += s.softBounces || 0;
    totalComplaints += s.complaints || s.spamReports || 0;
  }

  const openRate = totalDelivered > 0 ? totalOpens / totalDelivered : 0;
  const bounceRate =
    totalSent > 0 ? (totalHardBounces + totalSoftBounces) / totalSent : 0;
  const spamRate = totalDelivered > 0 ? totalComplaints / totalDelivered : 0;

  return {
    domain,
    campaigns: domainCampaigns.length,
    sent: totalSent,
    delivered: totalDelivered,
    opens: totalOpens,
    hardBounces: totalHardBounces,
    softBounces: totalSoftBounces,
    complaints: totalComplaints,
    openRate,
    bounceRate,
    spamRate,
  };
}

function getStatus(stats) {
  if (!stats || stats.sent === 0) return 'inactive';

  // Critical if ANY metric is critical
  if (
    stats.openRate < THRESHOLDS.openRate.warning ||
    stats.bounceRate > THRESHOLDS.bounceRate.critical ||
    stats.spamRate > THRESHOLDS.spamRate.critical
  ) {
    return 'critical';
  }

  // Warning if any metric is in warning zone
  if (
    stats.openRate < THRESHOLDS.openRate.healthy ||
    stats.bounceRate > THRESHOLDS.bounceRate.warning ||
    stats.spamRate > THRESHOLDS.spamRate.warning
  ) {
    return 'warning';
  }

  return 'healthy';
}

function statusEmoji(status) {
  return { healthy: '\u2705', warning: '\u26a0\ufe0f', critical: '\ud83d\udd34', inactive: '\u26aa' }[status] || '\u2753';
}

// ── Domain Authentication Check via Brevo API ──

async function checkDomainAuthentication() {
  const issues = [];
  try {
    const data = await brevoGet('/senders/domains');
    const domains = data.domains || [];

    for (const d of domains) {
      const name = d.domain_name;
      // Only check domains we actively send from
      if (!SENDER_DOMAINS.some(sd => sd === name || sd.endsWith('.' + name))) continue;

      if (!d.authenticated) {
        const missing = [];
        if (!d.dkim_record_status) missing.push('DKIM');
        if (!d.spf_record_status) missing.push('SPF');
        if (!d.dmarc_record_status) missing.push('DMARC');

        issues.push({
          domain: name,
          verified: d.verified || false,
          authenticated: false,
          missing,
          message: `${name}: NOT authenticated — missing ${missing.join(', ')}. Emails from this domain may go to spam.`,
        });
      }
    }

    // Check if any SENDER_DOMAINS are completely missing from Brevo
    const registeredRoots = domains.map(d => d.domain_name);
    for (const sd of SENDER_DOMAINS) {
      // Get root domain
      const parts = sd.split('.');
      const root = parts.length > 2 ? parts.slice(-2).join('.') : sd;
      if (!registeredRoots.includes(root) && !registeredRoots.includes(sd)) {
        issues.push({
          domain: sd,
          verified: false,
          authenticated: false,
          missing: ['NOT REGISTERED'],
          message: `${sd}: Not registered in Brevo at all — add and authenticate this domain.`,
        });
      }
    }
  } catch (err) {
    console.error('[sender-health] Domain auth check failed:', err.message);
    issues.push({
      domain: 'SYSTEM',
      verified: false,
      authenticated: false,
      missing: ['API_ERROR'],
      message: `Domain auth check failed: ${err.message}`,
    });
  }
  return issues;
}

async function run() {
  const results = [];

  // Check domain authentication FIRST — this is the most critical check
  const domainIssues = await checkDomainAuthentication();

  // Process domains in batches of 5 to avoid rate limits
  for (let i = 0; i < SENDER_DOMAINS.length; i += 5) {
    const batch = SENDER_DOMAINS.slice(i, i + 5);
    const batchResults = await Promise.all(
      batch.map((d) => getStatsForDomain(d))
    );
    results.push(...batchResults);
  }

  const active = results.filter((r) => r !== null);
  const inactive = SENDER_DOMAINS.length - active.length;

  // Build Slack blocks
  const today = new Date().toLocaleDateString('en-US', {
    month: 'long',
    day: 'numeric',
  });
  const blocks = [
    {
      type: 'header',
      text: {
        type: 'plain_text',
        text: `\ud83d\udcca Sender Health Report \u2014 ${today}`,
      },
    },
    { type: 'divider' },
  ];

  let hasCritical = false;
  let hasWarning = false;
  const actionItems = [];

  // Sort: critical first, then warning, then healthy
  const statusOrder = { critical: 0, warning: 1, healthy: 2 };
  active.sort((a, b) => statusOrder[getStatus(a)] - statusOrder[getStatus(b)]);

  for (const stats of active) {
    const status = getStatus(stats);
    const emoji = statusEmoji(status);

    if (status === 'critical') hasCritical = true;
    if (status === 'warning') hasWarning = true;

    const line =
      `${emoji} *${stats.domain}*\n` +
      `     \ud83d\udcec ${stats.delivered} delivered \u00b7 \ud83d\udcd6 ${(stats.openRate * 100).toFixed(1)}% open \u00b7 ` +
      `\u21a9\ufe0f ${(stats.bounceRate * 100).toFixed(1)}% bounce \u00b7 ` +
      `\ud83d\udeab ${stats.complaints} complaints`;

    blocks.push({
      type: 'section',
      text: { type: 'mrkdwn', text: line },
    });

    if (status === 'warning') {
      actionItems.push(
        `\u26a0\ufe0f *${stats.domain}* \u2014 Reduce volume to 50%, monitor 3 days`
      );
    }
    if (status === 'critical') {
      actionItems.push(
        `\ud83d\udd34 *${stats.domain}* \u2014 PAUSE IMMEDIATELY, swap sender needed`
      );
    }
  }

  if (inactive > 0) {
    blocks.push({
      type: 'context',
      elements: [
        {
          type: 'mrkdwn',
          text: `\u26aa ${inactive} senders inactive (no campaigns in last 7 days)`,
        },
      ],
    });
  }

  // Domain authentication issues
  if (domainIssues.length > 0) {
    hasCritical = true;
    blocks.push({ type: 'divider' });
    blocks.push({
      type: 'section',
      text: {
        type: 'mrkdwn',
        text: '*\ud83d\udd10 Domain Authentication Issues:*\n' +
          domainIssues.map(d => `\ud83d\udd34 ${d.message}`).join('\n'),
      },
    });
    // Add to action items
    for (const d of domainIssues) {
      actionItems.push(`\ud83d\udd34 *${d.domain}* — Add missing DNS records: ${d.missing.join(', ')}. PAUSE campaigns from this domain until fixed.`);
    }
  }

  if (actionItems.length > 0) {
    blocks.push({ type: 'divider' });
    blocks.push({
      type: 'section',
      text: {
        type: 'mrkdwn',
        text: '*\ud83d\udea8 Action Required:*\n' + actionItems.join('\n'),
      },
    });
  }

  // Post to Slack
  const alertPrefix = hasCritical ? '<!channel> ' : '';
  await slack.postBlocks(
    CHANNEL(),
    blocks,
    `${alertPrefix}Sender Health Report \u2014 ${today}`
  );

  // Log to Airtable
  try {
    const Airtable = require('airtable');
    const base = new Airtable({ apiKey: process.env.AIRTABLE_PAT }).base(
      process.env.AIRTABLE_BASE
    );

    for (const stats of active) {
      const status = getStatus(stats);
      await base('SenderHealth').create([
        {
          fields: {
            Domain: stats.domain,
            Date: new Date().toISOString().split('T')[0],
            OpenRate: Math.round(stats.openRate * 1000) / 10,
            BounceRate: Math.round(stats.bounceRate * 1000) / 10,
            SpamRate: Math.round(stats.spamRate * 10000) / 100,
            Delivered: stats.delivered,
            Sent: stats.sent,
            HardBounces: stats.hardBounces,
            Complaints: stats.complaints,
            Status: status,
          },
        },
      ]);
    }
  } catch (err) {
    console.error('[sender-health] Airtable log error:', err.message);
  }

  return { active: active.length, inactive, hasCritical, hasWarning };
}

module.exports = async (req, res) => {
  waitUntil(run().then(async () => {
    const { logCronRun } = require('../../shared/airtable');
    await logCronRun('sender-health').catch(e => console.error('[sender-health] logCronRun:', e.message));
  }));
  res.status(200).json({ ok: true, message: 'Sender health check started' });
};
