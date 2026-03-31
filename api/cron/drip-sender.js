/**
 * drip-sender.js — Daily cron that sends outreach emails via Brevo transactional API
 *
 * Reads from DripQueue Airtable table, sends the next drip step for each due contact,
 * updates the record with new step and next send date.
 *
 * Drip schedule:
 *   Step 1 (Initial):    Day 0   — sent the day after prospect is queued
 *   Step 2 (Follow-up):  Day 3   — 3 days after step 1
 *   Step 3 (Follow-up):  Day 7   — 7 days after step 1
 *   Step 4 (Final):      Day 14  — 14 days after step 1
 *
 * Runs daily at 9 AM UTC via Vercel cron.
 */
const { waitUntil } = require('@vercel/functions');
const slack = require('../../shared/slack');
const { TEMPLATES, fillTemplate, toHtml } = require('../../shared/templates');

const BREVO_KEY = process.env.BREVO_API_KEY;
const AIRTABLE_PAT = (process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || '').trim();
const AIRTABLE_BASE = (process.env.AIRTABLE_BASE || process.env.AIRTABLE_BASE_ID || '').trim();
const CHANNEL = () => process.env.CHANNEL_COMMAND_CENTER;

// Drip step delays (days after step 1)
const DRIP_DELAYS = { 1: 0, 2: 3, 3: 7, 4: 14 };

// Daily send limits per client
const MAX_PER_CLIENT = 55;

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─── Airtable Helpers ─────────────────────────────────────────────────────────

async function atFetch(endpoint, method = 'GET', body = null) {
  const opts = {
    method,
    headers: {
      'Authorization': `Bearer ${AIRTABLE_PAT}`,
      'Content-Type': 'application/json',
    },
  };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE}${endpoint}`, opts);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Airtable ${method} ${endpoint} ${res.status}: ${text.slice(0, 200)}`);
  }
  return res.json();
}

/**
 * Get all DripQueue records that are due for sending today
 */
async function getDueRecords() {
  const today = new Date().toISOString().split('T')[0];
  console.log(`[drip-sender] getDueRecords — today=${today}, base=${AIRTABLE_BASE}, pat=${AIRTABLE_PAT ? AIRTABLE_PAT.substring(0, 10) + '...' : 'MISSING'}`);
  const records = [];
  let offset = null;

  do {
    const params = new URLSearchParams({
      filterByFormula: `AND({Status}="active", {NextSendDate}<="${today}")`,
      pageSize: '100',
    });
    if (offset) params.set('offset', offset);

    const url = `/DripQueue?${params}`;
    console.log(`[drip-sender] Fetching: ${url.substring(0, 120)}`);
    const data = await atFetch(url);
    console.log(`[drip-sender] Got ${(data.records || []).length} records, offset=${data.offset || 'none'}`);
    records.push(...(data.records || []));
    offset = data.offset || null;
    if (offset) await sleep(200);
  } while (offset);

  return records.map(r => ({ id: r.id, ...r.fields }));
}

/**
 * Update a DripQueue record after sending
 */
async function updateDripRecord(recordId, fields) {
  return atFetch(`/DripQueue/${recordId}`, 'PATCH', { fields });
}

/**
 * Batch update DripQueue records (up to 10 at a time)
 */
async function batchUpdateDripRecords(updates) {
  for (let i = 0; i < updates.length; i += 10) {
    const batch = updates.slice(i, i + 10);
    await atFetch('/DripQueue', 'PATCH', {
      records: batch.map(u => ({ id: u.id, fields: u.fields })),
    });
    if (i + 10 < updates.length) await sleep(200);
  }
}

// ─── Brevo Transactional Send ─────────────────────────────────────────────────

async function sendTransactionalEmail(to, sender, subject, htmlContent, tags = []) {
  const res = await fetch('https://api.brevo.com/v3/smtp/email', {
    method: 'POST',
    headers: {
      'api-key': BREVO_KEY,
      'Content-Type': 'application/json',
      'Accept': 'application/json',
    },
    body: JSON.stringify({
      sender: { name: sender.name, email: sender.email },
      to: [{ email: to.email, name: to.name || to.email }],
      replyTo: { email: sender.email, name: sender.name },
      subject,
      htmlContent,
      tags,
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Brevo SMTP ${res.status}: ${text.slice(0, 200)}`);
  }

  return res.json();
}

// ─── Main Logic ───────────────────────────────────────────────────────────────

async function runDripSender() {
  const startTime = Date.now();
  const today = new Date().toISOString().split('T')[0];

  console.log(`[drip-sender] Starting — ${today}`);

  // 1. Get all due records
  const dueRecords = await getDueRecords();
  console.log(`[drip-sender] ${dueRecords.length} records due for sending`);

  if (dueRecords.length === 0) {
    return { sent: 0, errors: 0, skipped: 0 };
  }

  // 2. Group by client
  const byClient = {};
  for (const rec of dueRecords) {
    const slug = rec.ClientSlug || 'UNKNOWN';
    if (!byClient[slug]) byClient[slug] = [];
    byClient[slug].push(rec);
  }

  let totalSent = 0;
  let totalErrors = 0;
  let totalSkipped = 0;
  const clientSummaries = [];

  // 3. Process each client
  for (const [slug, records] of Object.entries(byClient)) {
    const template = TEMPLATES[slug];
    if (!template) {
      console.error(`[drip-sender] No template for ${slug}, skipping ${records.length} records`);
      totalSkipped += records.length;
      continue;
    }

    // Apply daily limit
    const toSend = records.slice(0, MAX_PER_CLIENT);
    const skipped = records.length - toSend.length;
    totalSkipped += skipped;

    let clientSent = 0;
    let clientErrors = 0;
    const updates = [];

    for (const rec of toSend) {
      const step = (rec.DripStep || 0); // current completed step
      const nextStep = step + 1;

      if (nextStep > 4) {
        // Already complete
        updates.push({ id: rec.id, fields: { Status: 'complete' } });
        continue;
      }

      const contact = {
        firstName: rec.FirstName || 'there',
        domain: rec.Domain || '',
        siteName: rec.SiteName || rec.Domain || '',
        articleTitle: rec.ArticleTitle || 'your recent content',
      };

      const subject = fillTemplate(template.subjects[nextStep - 1], contact);
      const bodyText = fillTemplate(template.bodies[nextStep - 1], contact);
      const html = toHtml(bodyText);

      const sender = {
        email: rec.SenderEmail,
        name: 'Yam Roar',
        id: rec.SenderId,
      };

      try {
        await sendTransactionalEmail(
          { email: rec.Email, name: contact.firstName },
          sender,
          subject,
          html,
          [slug, `drip-step-${nextStep}`, today],
        );

        // Calculate next send date
        let nextFields;
        if (nextStep >= 4) {
          nextFields = { DripStep: 4, Status: 'complete' };
        } else {
          const nextDelay = DRIP_DELAYS[nextStep + 1] - DRIP_DELAYS[nextStep];
          const nextDate = new Date();
          nextDate.setDate(nextDate.getDate() + nextDelay);
          nextFields = {
            DripStep: nextStep,
            NextSendDate: nextDate.toISOString().split('T')[0],
          };
        }

        updates.push({ id: rec.id, fields: nextFields });
        clientSent++;

        // Rate limit: ~2 emails/sec to stay under Brevo limits
        await sleep(500);
      } catch (err) {
        console.error(`[drip-sender] Error sending to ${rec.Email}: ${err.message}`);
        clientErrors++;

        // If it's a hard bounce (invalid email), mark as bounced
        if (err.message.includes('invalid') || err.message.includes('blocked')) {
          updates.push({ id: rec.id, fields: { Status: 'bounced' } });
        }
      }
    }

    // Batch update all records for this client
    if (updates.length > 0) {
      try {
        await batchUpdateDripRecords(updates);
      } catch (err) {
        console.error(`[drip-sender] Batch update error for ${slug}: ${err.message}`);
      }
    }

    totalSent += clientSent;
    totalErrors += clientErrors;

    if (clientSent > 0 || clientErrors > 0) {
      clientSummaries.push(`${slug}: ${clientSent} sent${clientErrors > 0 ? `, ${clientErrors} errors` : ''}${skipped > 0 ? `, ${skipped} deferred (limit)` : ''}`);
    }

    console.log(`[drip-sender] ${slug}: ${clientSent} sent, ${clientErrors} errors, ${skipped} deferred`);
  }

  // 4. Post summary to Slack
  const duration = ((Date.now() - startTime) / 1000).toFixed(1);
  let summary = `:mailbox_with_mail: *Drip Sender — ${today}*\n`;
  summary += `Sent: ${totalSent} | Errors: ${totalErrors} | Skipped: ${totalSkipped} | Duration: ${duration}s\n`;
  if (clientSummaries.length > 0) {
    summary += `\n${clientSummaries.map(s => `• ${s}`).join('\n')}`;
  }

  try {
    await slack.post(CHANNEL(), summary);
  } catch (err) {
    console.error(`[drip-sender] Slack post failed: ${err.message}`);
  }

  // Also post to Discord if configured
  try {
    const discord = require('../../shared/discord');
    await discord.postIfConfigured('command', summary);
  } catch { /* discord module may not exist */ }

  console.log(`[drip-sender] Complete — ${totalSent} sent, ${totalErrors} errors in ${duration}s`);

  return { sent: totalSent, errors: totalErrors, skipped: totalSkipped };
}

// ─── Vercel Handler ───────────────────────────────────────────────────────────

module.exports = async function handler(req, res) {
  if (req.method !== 'GET' && req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const result = await runDripSender();
    const { logCronRun } = require('../../shared/airtable');
    await logCronRun('drip-sender').catch(e => console.error('[drip-sender] logCronRun:', e.message));
    console.log(`[drip-sender] Done: ${JSON.stringify(result)}`);
    res.status(200).json({
      ok: true,
      ...result,
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    console.error('[drip-sender] Fatal error:', err);
    res.status(500).json({
      ok: false,
      error: err.message,
      timestamp: new Date().toISOString(),
    });
  }
};
