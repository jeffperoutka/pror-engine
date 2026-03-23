/**
 * Daily Digest Agent
 * Generates daily/weekly summaries of all activity across clients
 * Includes Gmail inbox summary (unread count, replies needing attention, auto-replies sent)
 */
const db = require('../shared/airtable');
const slack = require('../shared/slack');
const gmail = require('../shared/gmail');

// ── Gmail Inbox Summary ──────────────────────────────────────────────────────

async function getGmailSummary() {
  try {
    // Run all Gmail queries in parallel
    const [unreadCount, sentToday, recentMsgs] = await Promise.all([
      gmail.getUnreadCount(),
      gmail.getSentToday(),
      gmail.getRecentMessages(1, 30),
    ]);

    // Classify recent messages by type (quick heuristic — no AI call needed for digest)
    let repliesNeedingAttention = 0;
    let inboundPitches = 0;
    const keyEmails = [];

    for (const { id } of recentMsgs.slice(0, 15)) {
      try {
        const email = await gmail.getMessage(id);
        const subject = email.subject.toLowerCase();
        const from = email.from.toLowerCase();
        const isReply = subject.startsWith('re:') || subject.includes('re:');
        const isSpam = from.includes('noreply') || from.includes('no-reply') || from.includes('mailer-daemon');
        const isInbound = !isReply && !isSpam && (
          subject.includes('link') || subject.includes('guest post') ||
          subject.includes('partnership') || subject.includes('content')
        );

        if (isReply && email.labelIds.includes('UNREAD')) repliesNeedingAttention++;
        if (isInbound) inboundPitches++;

        if ((isReply || isInbound) && !isSpam) {
          keyEmails.push({
            from: email.from.replace(/<.*>/, '').trim().slice(0, 40),
            subject: email.subject.slice(0, 60),
            type: isReply ? 'reply' : 'inbound',
          });
        }
      } catch { /* skip individual message errors */ }
    }

    return {
      unread: unreadCount,
      autoRepliesSent: sentToday.length,
      repliesNeedingAttention,
      inboundPitches,
      keyEmails: keyEmails.slice(0, 6),
    };
  } catch (err) {
    console.error('[daily-digest] Gmail summary error:', err.message);
    return null;
  }
}

async function execute(args = {}) {
  const { channel } = args;
  const targetChannel = channel || process.env.CHANNEL_COMMAND_CENTER;

  if (!targetChannel) {
    console.error('No channel for digest — CHANNEL_COMMAND_CENTER not set');
    return;
  }

  try {
    const today = new Date();
    const dateStr = today.toISOString().split('T')[0];
    const month = dateStr.slice(0, 7);
    const dayOfMonth = today.getDate();

    const dayName = today.toLocaleDateString('en-US', { weekday: 'long' });
    const monthName = today.toLocaleDateString('en-US', { month: 'long' });

    // Pull all data (Gmail summary runs in parallel with client data)
    const gmailSummaryPromise = getGmailSummary();
    const clients = await db.getClients();

    if (!clients.length) {
      await slack.post(targetChannel, `📊 *Daily Digest — ${monthName} ${dayOfMonth} (${dayName})*\n\nNo active clients. Add clients to Airtable to start tracking.`);
      return;
    }

    // Gather per-client data
    const clientData = [];
    let totalLinksMonth = 0, totalLinksToday = 0;
    let totalRedditPosted = 0, totalRedditDrafted = 0;
    let totalRevenue = 0, totalCosts = 0;

    for (const c of clients) {
      const [links, reddit, finances] = await Promise.all([
        db.getLinks(c.Name, { month }).catch(() => []),
        db.getReddit(c.Name).catch(() => []),
        db.getFinances({ client: c.Name, month }).catch(() => []),
      ]);

      const linksToday = links.filter(l => l['Date Placed'] === dateStr).length;
      const linksMonth = links.length;
      const redditPosted = reddit.filter(r => r.Status === 'Posted').length;
      const redditDrafted = reddit.filter(r => r.Status === 'Drafted').length;
      const revenue = finances.filter(f => f.Type === 'Revenue').reduce((s, f) => s + (f.Amount || 0), 0);
      const costs = finances.filter(f => f.Type === 'Cost').reduce((s, f) => s + (f.Amount || 0), 0);

      totalLinksMonth += linksMonth;
      totalLinksToday += linksToday;
      totalRedditPosted += redditPosted;
      totalRedditDrafted += redditDrafted;
      totalRevenue += revenue;
      totalCosts += costs;

      clientData.push({
        name: c.Name,
        linksToday,
        linksMonth,
        redditPosted,
        redditDrafted,
        revenue,
        costs,
        margin: revenue ? ((1 - costs / revenue) * 100).toFixed(1) : '0.0',
        services: c.Services || 'N/A',
      });
    }

    const totalMargin = totalRevenue ? ((1 - totalCosts / totalRevenue) * 100).toFixed(1) : '0.0';

    // Resolve Gmail summary
    const gmailSummary = await gmailSummaryPromise;

    // Build digest message
    let msg = `📊 *PROR Engine — Daily Digest*\n`;
    msg += `_${monthName} ${dayOfMonth}, ${today.getFullYear()} (${dayName})_\n\n`;
    msg += `━━━━━━━━━━━━━━━━━━━━━━\n\n`;

    // Gmail Inbox section
    if (gmailSummary) {
      msg += `📧 *Gmail Inbox*\n`;
      msg += `• Unread: ${gmailSummary.unread}\n`;
      if (gmailSummary.repliesNeedingAttention > 0) {
        msg += `• ⚠️ ${gmailSummary.repliesNeedingAttention} replies needing attention\n`;
      }
      if (gmailSummary.inboundPitches > 0) {
        msg += `• 📥 ${gmailSummary.inboundPitches} inbound pitches\n`;
      }
      msg += `• 🤖 ${gmailSummary.autoRepliesSent} auto-replies sent today\n`;
      if (gmailSummary.keyEmails.length > 0) {
        msg += `  _Key emails:_\n`;
        for (const e of gmailSummary.keyEmails) {
          const icon = e.type === 'reply' ? '↩️' : '📥';
          msg += `    ${icon} ${e.from} — _${e.subject}_\n`;
        }
      }
      msg += '\n';
    }

    // Links section
    msg += `🔗 *Link Building*\n`;
    msg += `• Today: ${totalLinksToday} links placed\n`;
    msg += `• MTD: ${totalLinksMonth} links\n\n`;

    // Reddit section
    msg += `💬 *Reddit*\n`;
    msg += `• Posted: ${totalRedditPosted} comments/posts\n`;
    msg += `• Pending review: ${totalRedditDrafted} drafts\n\n`;

    // Finances
    msg += `💰 *Financials (${monthName})*\n`;
    msg += `• Revenue: $${totalRevenue.toLocaleString()}\n`;
    msg += `• Costs: $${totalCosts.toLocaleString()}\n`;
    msg += `• Margin: ${totalMargin}%\n\n`;

    msg += `━━━━━━━━━━━━━━━━━━━━━━\n\n`;

    // Client breakdown
    msg += `📋 *Client Breakdown*\n\n`;
    for (const cd of clientData) {
      const statusIcon = cd.revenue > 0 ? '✅' : '⬜';
      msg += `*${cd.name}:* ${cd.linksMonth} links, ${cd.redditPosted} reddit posted`;
      if (cd.revenue > 0) msg += ` | $${cd.revenue.toLocaleString()} rev`;
      msg += ` ${statusIcon}\n`;
    }

    // Action items
    const actionItems = [];
    if (totalRedditDrafted > 0) actionItems.push(`${totalRedditDrafted} Reddit drafts pending QA review`);
    for (const cd of clientData) {
      if (cd.linksMonth === 0 && cd.services?.includes('link')) {
        actionItems.push(`${cd.name}: No links placed yet this month`);
      }
    }

    if (actionItems.length) {
      msg += `\n━━━━━━━━━━━━━━━━━━━━━━\n\n`;
      msg += `⚠️ *Action Items*\n`;
      for (const item of actionItems) {
        msg += `• ${item}\n`;
      }
    }

    await slack.post(targetChannel, msg);

  } catch (err) {
    console.error('Digest error:', err);
    await slack.post(targetChannel || process.env.CHANNEL_COMMAND_CENTER, `❌ Daily digest error: ${err.message}`);
  }
}

module.exports = { execute };
