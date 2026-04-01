/**
 * Daily Digest Agent — matches the original link-building-bot format
 *
 * Shows:
 *   - Inbox opportunities (count by type)
 *   - Last 24h email activity (received, sent, key emails)
 *   - Weekend recap (Mondays only)
 *   - Links placed this month per client (from MASTERSHEETS)
 *   - Bot activity (outreach DB size)
 *   - AI-generated action items (Claude Haiku)
 */
const Airtable = require('airtable');
const slack = require('../shared/slack');
const discord = require('../shared/discord');
const gmail = require('../shared/gmail');
const ai = require('../shared/ai');

const AIRTABLE_API_KEY = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const MASTERSHEETS = 'appEGWJRxSrTv3IOL';
const PROR_DB = (process.env.AIRTABLE_BASE_ID || process.env.AIRTABLE_BASE || 'app3v0KJ4kQimscg3').trim();

function extractDomain(emailOrFrom) {
  const match = emailOrFrom.match(/@([a-z0-9.-]+\.[a-z]{2,})/i);
  if (!match) return null;
  const d = match[1].toLowerCase();
  const freemail = ['gmail.com','yahoo.com','hotmail.com','outlook.com','aol.com','icloud.com','protonmail.com','mail.com','zoho.com','yandex.com'];
  return freemail.includes(d) ? null : d;
}

// ── Airtable direct fetch (for MASTERSHEETS cross-base queries) ─────────────

async function atFetch(base, table, params = {}) {
  const qs = new URLSearchParams();
  if (params.filter) qs.set('filterByFormula', params.filter);
  if (params.pageSize) qs.set('pageSize', String(params.pageSize));
  if (params.fields) params.fields.forEach(f => qs.append('fields[]', f));
  const url = `https://api.airtable.com/v0/${base}/${table}?${qs}`;
  const r = await fetch(url, { headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` } });
  return r.json();
}

// ── Inbox opportunities ─────────────────────────────────────────────────────

async function getInboxOpportunities() {
  try {
    const messages = await gmail.listMessages('is:unread', 30);
    if (!messages.length) return { total: 0, replies: 0, inbound: 0, uniqueDomains: 0 };

    let replies = 0, inbound = 0;
    const domains = new Set();
    for (const { id } of messages.slice(0, 10)) {
      try {
        const email = await gmail.getMessage(id);
        const subject = email.subject.toLowerCase();
        const isReply = subject.startsWith('re:') || subject.includes('re:');
        const isInbound = !isReply && (
          subject.includes('link') || subject.includes('guest post') ||
          subject.includes('partnership') || subject.includes('collaboration')
        );
        if (isReply) replies++;
        if (isInbound) inbound++;
        const domain = extractDomain(email.from);
        if (domain && (isReply || isInbound)) domains.add(domain);
      } catch { /* skip */ }
    }

    return { total: messages.length, replies, inbound, uniqueDomains: domains.size };
  } catch {
    return { total: 0, replies: 0, inbound: 0, uniqueDomains: 0 };
  }
}

// ── Last 24h activity ───────────────────────────────────────────────────────

async function getYesterdayActivity() {
  try {
    const recentMsgs = await gmail.getRecentMessages(1, 50);
    const sentToday = await gmail.getSentToday();

    const received = [];
    for (const { id } of recentMsgs.slice(0, 20)) {
      try {
        const email = await gmail.getMessage(id);
        const subject = email.subject.toLowerCase();
        const from = email.from;
        const isSpam = from.includes('noreply') || from.includes('no-reply') || from.includes('mailer-daemon');
        const isReply = subject.startsWith('re:') || subject.includes('re:');
        received.push({
          from: email.from.replace(/<.*>/, '').trim().slice(0, 40),
          subject: email.subject.slice(0, 60),
          type: isSpam ? 'spam' : isReply ? 'reply' : 'inbound',
        });
      } catch { /* skip */ }
    }

    const uniqueDomains = new Set();
    for (const e of received) {
      if (e.type !== 'spam') {
        const domain = extractDomain(e.from);
        if (domain) uniqueDomains.add(domain);
      }
    }

    return {
      received: received.length,
      sent: sentToday.length,
      replies: received.filter(e => e.type === 'reply').length,
      inbound: received.filter(e => e.type === 'inbound').length,
      spam: received.filter(e => e.type === 'spam').length,
      uniqueDomains: uniqueDomains.size,
      recentEmails: received.filter(e => e.type !== 'spam').slice(0, 8),
    };
  } catch (err) {
    console.error('[daily-digest] getYesterdayActivity error:', err.message);
    return { received: 0, sent: 0, replies: 0, inbound: 0, spam: 0, recentEmails: [] };
  }
}

// ── Weekend recap (Mondays only) ────────────────────────────────────────────

async function getWeekendRecap() {
  const today = new Date();
  if (today.getUTCDay() !== 1) return null; // Only on Mondays

  try {
    const messages = await gmail.getRecentMessages(3, 50);
    if (!messages.length) return { total: 0, emails: [] };

    const emails = [];
    for (const { id } of messages.slice(0, 20)) {
      try {
        const email = await gmail.getMessage(id);
        const subject = email.subject.toLowerCase();
        const from = email.from;
        const isReply = subject.startsWith('re:') || subject.includes('re:');
        const isSpam = subject.includes('unsubscribe') || subject.includes('newsletter') ||
          from.includes('noreply') || from.includes('no-reply') || from.includes('mailer-daemon');
        const isInbound = !isReply && !isSpam && (
          subject.includes('link') || subject.includes('guest post') ||
          subject.includes('partnership') || subject.includes('collaboration') ||
          subject.includes('content') || subject.includes('article')
        );

        let type = 'other';
        if (isSpam) type = 'spam';
        else if (isReply) type = 'reply';
        else if (isInbound) type = 'inbound';

        emails.push({
          from: email.from.slice(0, 60),
          subject: email.subject.slice(0, 80),
          type,
        });
      } catch { /* skip */ }
    }

    return { total: messages.length, emails };
  } catch {
    return null;
  }
}

// ── Client progress from MASTERSHEETS ───────────────────────────────────────

async function getClientProgress() {
  try {
    const now = new Date();
    const ym = `${now.getUTCFullYear()}${String(now.getUTCMonth() + 1).padStart(2, '0')}`;
    const F = {
      client: 'fldRpfa8EfIV10uII',
      linkOrders: 'fldY7yvdggN4MgWLp',
      linksBuilt: 'fldP31u21GSAVl9NH',
      linksToBuild: 'fld9T5M3Pg3LWmgPj',
      yearMonth: 'fldaz1T5zAOyLQpjT',
    };
    const data = await atFetch(MASTERSHEETS, 'tbl5t5zpjIK8tySe7', {
      filter: `{${F.yearMonth}}='${ym}'`,
      fields: Object.values(F),
      pageSize: 50,
    });
    return (data.records || [])
      .filter(r => r.cellValuesByFieldId?.[F.client])
      .map(r => {
        const c = r.cellValuesByFieldId;
        return {
          name: c[F.client],
          ordered: Number(c[F.linkOrders]) || 0,
          built: Number(c[F.linksBuilt]) || 0,
          remaining: Number(c[F.linksToBuild]) || 0,
        };
      });
  } catch (err) {
    console.error('[daily-digest] getClientProgress error:', err.message);
    return [];
  }
}

// ── Bot activity (outreach DB size) ─────────────────────────────────────────

async function getBotActivity() {
  try {
    const [outreach, dripQueue] = await Promise.all([
      atFetch(PROR_DB, 'OUTREACH', { pageSize: 1 }),
      atFetch(PROR_DB, 'tbl8wLrkyZdM4f9nU', { pageSize: 1 }),
    ]);
    return {
      totalOutreach: outreach.totalRecords || 0,
      dripQueueTotal: dripQueue.totalRecords || 0,
    };
  } catch {
    return { totalOutreach: 0, dripQueueTotal: 0 };
  }
}

// ── Prospect pipeline (new sites added as opportunities) ────────────────────

async function getProspectPipeline() {
  try {
    const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    const thisMonth = new Date().toISOString().slice(0, 7);

    const [week, month] = await Promise.all([
      atFetch(PROR_DB, 'OutreachCosts', {
        filter: `IS_AFTER({Date}, "${sevenDaysAgo}")`,
      }),
      atFetch(PROR_DB, 'OutreachCosts', {
        filter: `DATETIME_FORMAT({Date}, 'YYYY-MM') = "${thisMonth}"`,
      }),
    ]);

    const weekRecords = week.records || [];
    const monthRecords = month.records || [];

    const newProspects7d = weekRecords.reduce((s, r) => s + (r.fields?.NewProspects || 0), 0);
    const newProspectsMonth = monthRecords.reduce((s, r) => s + (r.fields?.NewProspects || 0), 0);

    // Per-client breakdown
    const byClient = {};
    for (const r of weekRecords) {
      const client = r.fields?.Client || 'Unknown';
      byClient[client] = (byClient[client] || 0) + (r.fields?.NewProspects || 0);
    }

    return { newProspects7d, newProspectsMonth, dailyAvg: Math.round(newProspects7d / 7), byClient };
  } catch (err) {
    console.error('[daily-digest] getProspectPipeline error:', err.message);
    return { newProspects7d: 0, newProspectsMonth: 0, dailyAvg: 0, byClient: {} };
  }
}

// ── AI-generated action items ───────────────────────────────────────────────

async function generateActionItems(clients, inbox) {
  const context = {
    clients: clients.map(c => ({
      name: c.name,
      placed: `${c.built}/${c.ordered}`,
      remaining: c.remaining,
      status: c.built >= c.ordered ? 'on_track' : c.remaining > 0 ? 'behind' : 'in_progress',
    })),
    inboxTotal: inbox.total,
    inboxReplies: inbox.replies,
  };

  const prompt = `You are the daily briefing assistant for a link building agency. Generate 2-4 smart action items for Jeff based on this data. Focus on decisions that need human judgment — over-margin situations, schedule risks, opportunities. Be specific and direct. Max 12 words per item.

Data: ${JSON.stringify(context)}

Respond with ONLY a JSON array of strings, e.g.:
["designscene.net quoted $320 vs $240 ceiling — 2 links ahead, find replacement?", "Kobo behind by 2 links with 8 days left — approve budget flex?"]`;

  try {
    const items = await ai.json(prompt, 'You are a concise briefing assistant.', { model: 'claude-haiku-4-5-20251001' });
    return Array.isArray(items) ? items : [];
  } catch {
    return [];
  }
}

// ── Build the digest message (matches original format) ──────────────────────

function buildDigest(date, inbox, clients, activity, actionItems, weekendRecap, yesterday, pipeline) {
  const days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  const d = new Date(date);
  const dateStr = `${days[d.getDay()]}, ${months[d.getMonth()]} ${d.getDate()}`;

  const lines = [`🌅 *PROR Daily Digest — ${dateStr}*`, ''];

  // Inbox
  const opTotal = inbox.replies + inbox.inbound;
  const uniqueLabel = inbox.uniqueDomains > 0 ? ` from ${inbox.uniqueDomains} unique site${inbox.uniqueDomains === 1 ? '' : 's'}` : '';
  lines.push(`📧 *Inbox — ${opTotal > 0 ? opTotal + ' opportunities' + uniqueLabel : 'all clear'}*`);
  if (inbox.replies > 0) lines.push(`  • ↩️ ${inbox.replies} repl${inbox.replies === 1 ? 'y' : 'ies'} to outreach`);
  if (inbox.inbound > 0) lines.push(`  • 📥 ${inbox.inbound} inbound pitch${inbox.inbound === 1 ? '' : 'es'}`);
  const other = inbox.total - inbox.replies - inbox.inbound;
  if (other > 0) lines.push(`  • 📬 ${other} other (processed)`);
  if (opTotal === 0) lines.push(`  • Nothing actionable overnight`);
  lines.push('');

  // Yesterday's activity
  if (yesterday && (yesterday.received > 0 || yesterday.sent > 0)) {
    lines.push(`📬 *Last 24h Activity*`);
    const domainNote = yesterday.uniqueDomains > 0 ? ` · ${yesterday.uniqueDomains} unique sites` : '';
    lines.push(`  • ${yesterday.received} emails received (${yesterday.replies} replies, ${yesterday.inbound} inbound, ${yesterday.spam} spam)${domainNote}`);
    if (yesterday.sent > 0) lines.push(`  • ${yesterday.sent} auto-replies sent by bot`);
    if (yesterday.recentEmails.length > 0) {
      lines.push(`  *Key emails:*`);
      for (const e of yesterday.recentEmails) {
        const icon = e.type === 'reply' ? '↩️' : '📥';
        lines.push(`    ${icon} ${e.from} — _${e.subject}_`);
      }
    }
    lines.push('');
  }

  // Weekend recap (Mondays only)
  if (weekendRecap && weekendRecap.total > 0) {
    const wrReplies = weekendRecap.emails.filter(e => e.type === 'reply');
    const wrInbound = weekendRecap.emails.filter(e => e.type === 'inbound');
    const wrSpam = weekendRecap.emails.filter(e => e.type === 'spam');
    const wrOther = weekendRecap.emails.filter(e => e.type === 'other');

    lines.push(`📅 *Weekend Recap* (${weekendRecap.total} emails processed)`);
    if (wrReplies.length > 0) {
      lines.push(`  ↩️ *${wrReplies.length} replies:*`);
      for (const e of wrReplies.slice(0, 5)) {
        lines.push(`    • ${e.from} — _${e.subject}_`);
      }
      if (wrReplies.length > 5) lines.push(`    _...and ${wrReplies.length - 5} more_`);
    }
    if (wrInbound.length > 0) {
      lines.push(`  📥 *${wrInbound.length} inbound:*`);
      for (const e of wrInbound.slice(0, 5)) {
        lines.push(`    • ${e.from} — _${e.subject}_`);
      }
    }
    if (wrSpam.length > 0) lines.push(`  🗑️ ${wrSpam.length} spam/auto-reply (auto-handled)`);
    if (wrOther.length > 0) lines.push(`  📬 ${wrOther.length} other`);
    lines.push('');
  }

  // Client progress
  if (clients.length > 0) {
    lines.push(`🔗 *Links This Month*`);
    for (const c of clients) {
      const pct = c.ordered > 0 ? Math.round((c.built / c.ordered) * 100) : 0;
      const bar = pct >= 100 ? '✅' : pct >= 60 ? '🟡' : '🔴';
      lines.push(`  • ${bar} *${c.name}:* ${c.built}/${c.ordered} placed${c.remaining > 0 ? ` · ${c.remaining} pending` : ''}`);
    }
    lines.push('');
  }

  // Prospect pipeline
  if (pipeline && (pipeline.newProspects7d > 0 || pipeline.newProspectsMonth > 0)) {
    lines.push(`🎯 *Prospect Pipeline*`);
    lines.push(`  • New sites added (7d): ${pipeline.newProspects7d} | ~${pipeline.dailyAvg}/day`);
    lines.push(`  • New sites this month: ${pipeline.newProspectsMonth}`);
    const entries = Object.entries(pipeline.byClient || {}).filter(([, v]) => v > 0);
    if (entries.length > 0) {
      lines.push(`  • Per client: ${entries.map(([k, v]) => `${k}: +${v}`).join(', ')}`);
    }
    lines.push('');
  }

  // Bot activity
  lines.push(`📊 *Bot Activity*`);
  if (activity.dripQueueTotal > 0) lines.push(`  • ${activity.dripQueueTotal.toLocaleString()} unique sites in DripQueue`);
  lines.push(`  • ${activity.totalOutreach.toLocaleString()} total domains in outreach database`);
  lines.push('');

  // Action items
  if (actionItems.length > 0) {
    lines.push(`⚡ *Decisions needed:*`);
    actionItems.forEach((item, i) => lines.push(`  ${i + 1}. ${item}`));
  }

  return lines.join('\n');
}

// ── Execute ─────────────────────────────────────────────────────────────────

async function execute(args = {}) {
  const { channel } = args;
  const targetChannel = channel || process.env.CHANNEL_LINK_BUILDING || process.env.CHANNEL_COMMAND_CENTER;

  if (!targetChannel) {
    console.error('No channel for digest — set CHANNEL_LINK_BUILDING or CHANNEL_COMMAND_CENTER');
    return;
  }

  try {
    // Run all data fetches in parallel
    const [inbox, clients, activity, weekendRecap, yesterday, pipeline] = await Promise.all([
      getInboxOpportunities(),
      getClientProgress(),
      getBotActivity(),
      getWeekendRecap(),
      getYesterdayActivity(),
      getProspectPipeline(),
    ]);

    // Generate AI action items (needs inbox + client data)
    const actionItems = await generateActionItems(clients, inbox);

    // Build and send
    const message = buildDigest(new Date(), inbox, clients, activity, actionItems, weekendRecap, yesterday, pipeline);
    await slack.post(targetChannel, message);
    await discord.postIfConfigured('command', message);

  } catch (err) {
    console.error('Digest error:', err);
    await slack.post(targetChannel || process.env.CHANNEL_COMMAND_CENTER, `❌ Daily digest error: ${err.message}`);
    await discord.postIfConfigured('command', `❌ Daily digest error: ${err.message}`);
  }
}

module.exports = { execute };
