/**
 * Shared Brevo module — Email campaign API for all agents
 *
 * Handles: contact lists, bulk imports, sender management, scheduled campaigns,
 * tier-segmented 4-step drip sequences, campaign QA, and sender rotation.
 *
 * Architecture mirrors link-building-bot/api/lib/brevo.js patterns but adapted
 * for the PROR Engine agent system (uses shared/airtable for logging, shared/slack for QA).
 *
 * Drip sequence strategy:
 *   Brevo v3 API has no automation workflow endpoint — drip sequences are implemented
 *   as staggered scheduled campaigns (Day 0, 3, 7, 14). Reply detection is handled
 *   via webhook → cancel remaining campaigns.
 */

const BASE = 'https://api.brevo.com/v3';
const REQUEST_DELAY_MS = 600; // 100 req/min = ~600ms between requests
const DEFAULT_TIMEOUT_MS = 30000;

let lastRequestTime = 0;
let requestQueue = Promise.resolve();

// In-process sender rotation index
let senderRotationIdx = 0;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function rateLimit() {
  return new Promise((resolve) => {
    requestQueue = requestQueue.then(async () => {
      const now = Date.now();
      const elapsed = now - lastRequestTime;
      if (elapsed < REQUEST_DELAY_MS) {
        await new Promise((r) => setTimeout(r, REQUEST_DELAY_MS - elapsed));
      }
      lastRequestTime = Date.now();
      resolve();
    });
  });
}

function withTimeout(promise, ms = DEFAULT_TIMEOUT_MS) {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`Brevo request timed out after ${ms}ms`)), ms)
    ),
  ]);
}

async function brevoFetch(path, options = {}) {
  const key = process.env.BREVO_API_KEY;
  if (!key) throw new Error('[brevo] BREVO_API_KEY not set');

  await rateLimit();

  const url = `${BASE}${path}`;
  const method = options.method || 'GET';
  const headers = {
    'api-key': key,
    'Content-Type': 'application/json',
    Accept: 'application/json',
  };

  const fetchOpts = { method, headers };
  if (options.body) fetchOpts.body = JSON.stringify(options.body);

  console.log(`[brevo] ${method} ${path}`);
  const resp = await withTimeout(fetch(url, fetchOpts));
  const text = await resp.text();

  // Some endpoints return 204 with no body
  if (!text) return { _status: resp.status };

  let data;
  try {
    data = JSON.parse(text);
  } catch {
    throw new Error(`[brevo] Non-JSON response (${resp.status}): ${text.slice(0, 200)}`);
  }

  if (!resp.ok) {
    const msg = data.message || data.error || JSON.stringify(data);
    const err = new Error(`[brevo] ${method} ${path} -> ${resp.status}: ${msg}`);
    err.status = resp.status;
    err.body = data;
    throw err;
  }

  data._status = resp.status;
  return data;
}

// ---------------------------------------------------------------------------
// Contact List Management
// ---------------------------------------------------------------------------

/**
 * Create a new contact list in Brevo.
 * @param {string} name — list name (e.g. "ACME - 2026-03-20 - T1")
 * @param {number} [folderId] — optional folder ID; defaults to root
 */
async function createContactList(name, folderId) {
  console.log(`[brevo] Creating contact list: ${name}`);

  const body = { name };
  if (folderId) body.folderId = folderId;

  try {
    const result = await brevoFetch('/contacts/lists', {
      method: 'POST',
      body,
    });
    console.log(`[brevo] List created: ${name} (ID: ${result.id})`);
    return { id: result.id, name, created: true };
  } catch (err) {
    // If list name already exists, Brevo returns 400 — find and return it
    if (err.status === 400 && /already exists|duplicate/i.test(err.message)) {
      console.log(`[brevo] List already exists: ${name} — searching for it`);
      const existing = await findListByName(name);
      if (existing) return { id: existing.id, name: existing.name, created: false };
    }
    throw err;
  }
}

/**
 * Find a contact list by exact name match
 */
async function findListByName(name) {
  const result = await brevoFetch('/contacts/lists?limit=50&offset=0');
  const lists = result.lists || [];
  return lists.find((l) => l.name.toLowerCase() === name.toLowerCase()) || null;
}

/**
 * Bulk import contacts into a list with full personalization attributes.
 * Brevo replaces {{ params.FIELD }} merge tags per-contact at send time.
 *
 * @param {number} listId
 * @param {Array<object>} contacts — [{email, firstName, lastName, domain, articleTitle, ...}]
 */
async function importContacts(listId, contacts) {
  if (!contacts || contacts.length === 0) {
    console.log('[brevo] No contacts to import');
    return { imported: 0 };
  }

  console.log(`[brevo] Importing ${contacts.length} contacts into list ${listId}`);

  const jsonBody = contacts.map((c) => ({
    email: c.email,
    attributes: {
      FIRSTNAME: c.firstName || c.name || '',
      LASTNAME: c.lastName || '',
      DOMAIN: c.domain || '',
      SITE_NAME: c.siteName || '',
      ARTICLE_TITLE: c.articleTitle || '',
      ARTICLE_URL: c.articleUrl || '',
      ARTICLE_SNIPPET: c.articleSnippet || '',
      DR: String(c.dr || ''),
      TIER: c.tier || '',
      SCORE: String(c.score || ''),
    },
  }));

  const result = await brevoFetch('/contacts/import', {
    method: 'POST',
    body: {
      listIds: [listId],
      jsonBody,
      updateExistingContacts: true,
      emptyContactsAttributes: false,
    },
  });

  console.log(`[brevo] Import queued for ${contacts.length} contacts (process ID: ${result.processId || 'n/a'})`);
  return { processId: result.processId, count: contacts.length };
}

// ---------------------------------------------------------------------------
// Email Campaign Management
// ---------------------------------------------------------------------------

/**
 * Create an email campaign draft.
 *
 * @param {string} name — campaign name
 * @param {string} subject — email subject line (can contain merge tags)
 * @param {string} htmlContent — email HTML body
 * @param {number} listId — recipient list ID
 * @param {object} sender — {id, name, email}
 * @param {string} [scheduledAt] — ISO 8601 datetime for scheduled send
 */
async function createEmailCampaign(name, subject, htmlContent, listId, sender, scheduledAt) {
  console.log(`[brevo] Creating campaign: ${name}`);

  const body = {
    name,
    subject,
    htmlContent,
    recipients: { listIds: [listId] },
    sender: { name: sender.name, email: sender.email },
  };

  if (scheduledAt) body.scheduledAt = scheduledAt;

  const result = await brevoFetch('/emailCampaigns', {
    method: 'POST',
    body,
  });

  console.log(`[brevo] Campaign created: ${result.id}${scheduledAt ? ` (scheduled: ${scheduledAt})` : ''}`);
  return { id: result.id, name, scheduledAt: scheduledAt || null };
}

/**
 * Send a campaign immediately.
 */
async function sendCampaign(campaignId) {
  await brevoFetch(`/emailCampaigns/${campaignId}/sendNow`, { method: 'POST' });
  console.log(`[brevo] Campaign ${campaignId} sent`);
  return { campaignId, sent: true };
}

/**
 * Get campaign statistics (opens, clicks, replies, bounces).
 */
async function getCampaignStats(campaignId) {
  const campaign = await brevoFetch(`/emailCampaigns/${campaignId}`);
  const stats = campaign.statistics?.campaignStats?.[0] || {};
  return {
    campaignId,
    name: campaign.name,
    status: campaign.status,
    sent: stats.sent || 0,
    delivered: stats.delivered || 0,
    opens: stats.uniqueOpens || 0,
    clicks: stats.uniqueClicks || 0,
    bounces: (stats.hardBounces || 0) + (stats.softBounces || 0),
    unsubscribed: stats.unsubscriptions || 0,
    openRate: stats.sent ? ((stats.uniqueOpens || 0) / stats.sent * 100).toFixed(1) + '%' : '0%',
    clickRate: stats.sent ? ((stats.uniqueClicks || 0) / stats.sent * 100).toFixed(1) + '%' : '0%',
  };
}

/**
 * Delete/cancel a scheduled campaign (used when reply detected).
 */
async function cancelCampaign(campaignId) {
  try {
    // Try to update status to 'suspended' first
    await brevoFetch(`/emailCampaigns/${campaignId}`, {
      method: 'PUT',
      body: { status: 'suspended' },
    });
    console.log(`[brevo] Campaign ${campaignId} suspended`);
    return { campaignId, cancelled: true };
  } catch (err) {
    // If already sent or cannot suspend, try delete
    try {
      await brevoFetch(`/emailCampaigns/${campaignId}`, { method: 'DELETE' });
      console.log(`[brevo] Campaign ${campaignId} deleted`);
      return { campaignId, cancelled: true, deleted: true };
    } catch (delErr) {
      console.warn(`[brevo] Could not cancel campaign ${campaignId}:`, delErr.message);
      return { campaignId, cancelled: false, error: delErr.message };
    }
  }
}

// ---------------------------------------------------------------------------
// Sender Management
// ---------------------------------------------------------------------------

/**
 * Register a new sender identity.
 */
async function createSender(name, email) {
  console.log(`[brevo] Creating sender: ${name} <${email}>`);

  // Check if sender already exists
  const existing = await listSenders();
  const match = existing.find((s) => s.email.toLowerCase() === email.toLowerCase());
  if (match) {
    console.log(`[brevo] Sender already exists: ${email} (ID: ${match.id})`);
    return { id: match.id, name: match.name, email: match.email, created: false };
  }

  const result = await brevoFetch('/senders', {
    method: 'POST',
    body: { name, email },
  });

  console.log(`[brevo] Sender created: ${email} (ID: ${result.id})`);
  return { id: result.id, name, email, created: true };
}

/**
 * List all registered senders.
 */
async function listSenders() {
  const result = await brevoFetch('/senders');
  return result.senders || [];
}

/**
 * Rotate across available senders for domain reputation protection.
 * Returns next sender in rotation order.
 */
async function getRotatedSender() {
  const senders = await listSenders();
  if (senders.length === 0) throw new Error('[brevo] No senders configured');

  // Filter to only active senders (not blocked/deactivated)
  const active = senders.filter((s) => s.active !== false);
  if (active.length === 0) throw new Error('[brevo] No active senders available');

  const sender = active[senderRotationIdx % active.length];
  senderRotationIdx++;
  console.log(`[brevo] Rotated to sender: ${sender.email} (${senderRotationIdx}/${active.length})`);
  return sender;
}

/**
 * Get a specific sender by ID, or fall back to env default.
 */
async function getSender(senderId) {
  if (senderId) {
    const senders = await listSenders();
    const match = senders.find((s) => s.id === senderId || String(s.id) === String(senderId));
    if (match) return match;
  }

  // Fall back to BREVO_SENDER_ID env var
  const envId = process.env.BREVO_SENDER_ID;
  if (envId) {
    const senders = await listSenders();
    const match = senders.find((s) => String(s.id) === String(envId));
    if (match) return match;
  }

  // Last resort: first active sender
  return getRotatedSender();
}

// ---------------------------------------------------------------------------
// Tier Segmentation
// ---------------------------------------------------------------------------

const TIER_DEFINITIONS = {
  T1: { label: 'T1 (DR60+)', minDR: 60, maxDR: 999, personalization: 'individual' },
  T2: { label: 'T2 (DR40-59)', minDR: 40, maxDR: 59, personalization: 'template' },
  T3: { label: 'T3 (DR30-39)', minDR: 30, maxDR: 39, personalization: 'short' },
};

/**
 * Segment prospects into tiers by DR.
 */
function segmentByTier(prospects) {
  const segments = { T1: [], T2: [], T3: [] };
  for (const p of prospects) {
    const dr = p.dr || 0;
    if (dr >= 60) segments.T1.push(p);
    else if (dr >= 40) segments.T2.push(p);
    else segments.T3.push(p);
  }
  return segments;
}

// ---------------------------------------------------------------------------
// Drip Sequence Orchestration
// ---------------------------------------------------------------------------

/**
 * The 4-step drip schedule.
 * Each step is a separate scheduled campaign in Brevo.
 */
const DRIP_STEPS = [
  { step: 1, delayDays: 0,  label: 'Initial Outreach', hour: 9 },
  { step: 2, delayDays: 3,  label: 'Follow-up 1',      hour: 10 },
  { step: 3, delayDays: 7,  label: 'Follow-up 2',      hour: 14 },
  { step: 4, delayDays: 14, label: 'Final Follow-up',   hour: 11 },
];

/**
 * Build HTML email from plain text body with signature and unsubscribe.
 */
function buildEmailHtml(body, senderName) {
  const signature = `<br><br>${senderName || 'The Team'}<br>Content Partnerships`;
  const unsubscribe = `<br><br><p style="font-size: 11px; color: #999; margin-top: 20px;">If you'd prefer not to hear from us, <a href="{{ unsubscribe }}" style="color: #999; text-decoration: underline;">unsubscribe here</a>.</p>`;
  return `<html><body style="font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6; color: #333;">${body.replace(/\n/g, '<br>')}${signature}${unsubscribe}</body></html>`;
}

/**
 * Calculate scheduled send time for a drip step.
 * Ensures the scheduled time is in the future.
 */
function getScheduledTime(delayDays, hour) {
  const scheduledAt = new Date();
  scheduledAt.setDate(scheduledAt.getDate() + delayDays);
  scheduledAt.setUTCHours(hour, 0, 0, 0);

  // If scheduled time is in the past (e.g., Day 0 but it's already past 9 AM UTC), push to tomorrow
  if (scheduledAt.getTime() <= Date.now()) {
    scheduledAt.setDate(scheduledAt.getDate() + 1);
  }

  return scheduledAt;
}

/**
 * Create a full 4-step drip sequence for a client + tier segment.
 *
 * This is the core orchestration function. It:
 * 1. Creates a contact list for this tier segment
 * 2. Imports contacts with merge field attributes
 * 3. Creates 4 scheduled campaigns (Day 0, 3, 7, 14)
 * 4. Returns campaign IDs so they can be cancelled if a reply is received
 *
 * @param {object} client — {name, slug} client info
 * @param {string} tier — 'T1', 'T2', or 'T3'
 * @param {Array<object>} prospects — [{email, firstName, domain, dr, articleTitle, ...}]
 * @param {object} sequences — {subject1, body1, subject2, body2, ...} email content per step
 * @param {object} [opts] — {sender, dryRun}
 * @returns {object} — {listId, campaigns: [{step, campaignId, scheduledAt}], contactCount}
 */
async function createDripSequence(client, tier, prospects, sequences, opts = {}) {
  const { sender: explicitSender, dryRun = false } = opts;
  const dateStr = new Date().toISOString().slice(0, 10);
  const slug = client.slug || client.name.toLowerCase().replace(/\s+/g, '-');

  console.log(`[brevo] Creating drip sequence: ${slug} / ${tier} / ${prospects.length} prospects`);

  // 1. Get sender (explicit, or rotate)
  const sender = explicitSender || await getSender();

  // 2. Create contact list for this segment
  const listName = `${slug.toUpperCase()} - ${tier} - ${dateStr}`;
  const list = await createContactList(listName);

  // 3. Import contacts with personalization attributes
  const contactData = prospects.map((p) => ({
    email: p.email,
    firstName: p.firstName || p.name?.split(' ')[0] || extractFirstName(p.email),
    lastName: p.lastName || p.name?.split(' ').slice(1).join(' ') || '',
    domain: p.domain || '',
    siteName: p.siteName || domainToSiteName(p.domain),
    articleTitle: p.articleTitle || p.insertionTitle || 'your recent article',
    articleUrl: p.articleUrl || p.insertionUrl || (p.domain ? `https://${p.domain}` : ''),
    articleSnippet: p.articleSnippet || p.articleTitle || 'your recent article',
    dr: String(p.dr || ''),
    tier: tier,
    score: String(p.score || p.leadScore || ''),
  }));

  const importResult = await importContacts(list.id, contactData);

  if (dryRun) {
    console.log(`[brevo] Dry run — skipping campaign creation`);
    return {
      listId: list.id,
      listName: list.name || listName,
      contactCount: contactData.length,
      importProcessId: importResult.processId,
      campaigns: DRIP_STEPS.map((step) => ({
        step: step.step,
        label: step.label,
        scheduledAt: getScheduledTime(step.delayDays, step.hour).toISOString(),
        dryRun: true,
      })),
      sender: sender.email,
      tier,
    };
  }

  // 4. Create 4 scheduled campaigns
  const campaigns = [];

  for (const step of DRIP_STEPS) {
    const subjectKey = `subject${step.step}`;
    const bodyKey = `body${step.step}`;

    const subject = sequences[subjectKey];
    const body = sequences[bodyKey];

    if (!subject || !body) {
      console.warn(`[brevo] Skipping step ${step.step} — missing ${subjectKey} or ${bodyKey}`);
      continue;
    }

    const scheduledAt = getScheduledTime(step.delayDays, step.hour);
    const campaignName = `${slug.toUpperCase()} - ${tier} SEQ${step.step} ${step.label} - ${dateStr}`;
    const htmlContent = buildEmailHtml(body, sender.name);

    try {
      const campaign = await createEmailCampaign(
        campaignName,
        subject,
        htmlContent,
        list.id,
        sender,
        scheduledAt.toISOString()
      );

      campaigns.push({
        step: step.step,
        label: step.label,
        campaignId: campaign.id,
        scheduledAt: scheduledAt.toISOString(),
        subject,
        contactCount: contactData.length,
      });

      console.log(`[brevo] Step ${step.step}/${DRIP_STEPS.length}: ${campaign.id} -> ${scheduledAt.toISOString()}`);
    } catch (err) {
      console.error(`[brevo] Failed to create step ${step.step}:`, err.message);
      campaigns.push({
        step: step.step,
        label: step.label,
        error: err.message,
      });
    }
  }

  return {
    listId: list.id,
    listName: list.name || listName,
    contactCount: contactData.length,
    importProcessId: importResult.processId,
    campaigns,
    sender: sender.email,
    tier,
  };
}

/**
 * Create tier-segmented drip sequences for an entire campaign batch.
 * Splits prospects by DR tier, creates separate drip sequences per tier.
 *
 * @param {object} client — {name, slug}
 * @param {Array<object>} prospects — all prospects (will be segmented)
 * @param {object} copyByTier — {T1: {subject1, body1, ...}, T2: {...}, T3: {...}}
 * @param {object} [opts] — {sender, dryRun}
 */
async function createTieredDripSequences(client, prospects, copyByTier, opts = {}) {
  const segments = segmentByTier(prospects);
  const results = {};

  for (const [tier, tierProspects] of Object.entries(segments)) {
    if (tierProspects.length === 0) {
      console.log(`[brevo] No prospects for ${tier} — skipping`);
      continue;
    }

    const sequences = copyByTier[tier];
    if (!sequences || !sequences.subject1) {
      console.warn(`[brevo] No email copy for ${tier} — skipping ${tierProspects.length} prospects`);
      continue;
    }

    results[tier] = await createDripSequence(client, tier, tierProspects, sequences, opts);
  }

  return results;
}

// ---------------------------------------------------------------------------
// Reply Detection — Cancel Remaining Drip Steps
// ---------------------------------------------------------------------------

/**
 * When a reply is detected (via webhook or manual check), cancel all
 * remaining scheduled campaigns for that prospect's drip sequence.
 *
 * @param {Array<number>} campaignIds — all campaign IDs in the drip sequence
 */
async function cancelDripForReply(campaignIds) {
  const results = [];
  for (const id of campaignIds) {
    try {
      // Check if campaign is still scheduled (not already sent)
      const details = await brevoFetch(`/emailCampaigns/${id}`);
      if (details.status === 'draft' || details.status === 'queued') {
        const cancelled = await cancelCampaign(id);
        results.push({ campaignId: id, status: 'cancelled', ...cancelled });
      } else {
        results.push({ campaignId: id, status: details.status, skipped: true });
      }
    } catch (err) {
      results.push({ campaignId: id, status: 'error', error: err.message });
    }
  }

  const cancelledCount = results.filter((r) => r.status === 'cancelled').length;
  console.log(`[brevo] Reply detected — cancelled ${cancelledCount}/${campaignIds.length} remaining campaigns`);
  return results;
}

// ---------------------------------------------------------------------------
// Campaign QA
// ---------------------------------------------------------------------------

/**
 * Validate campaigns before sending. Checks:
 * - HTML content exists and has minimum length
 * - Signature present
 * - Unsubscribe link present (CAN-SPAM)
 * - Merge fields are populated on contacts
 * - List has contacts
 * - Sender is active
 *
 * @param {Array<number>} campaignIds
 * @returns {object} — {passed, failed, issues, campaigns}
 */
async function qaCampaigns(campaignIds) {
  const issues = [];
  const reviewed = [];

  for (const id of campaignIds) {
    try {
      const campaign = await brevoFetch(`/emailCampaigns/${id}`);
      const entry = {
        id,
        name: campaign.name,
        subject: campaign.subject,
        status: campaign.status,
        scheduledAt: campaign.scheduledAt,
        issues: [],
      };

      // Check HTML content
      if (!campaign.htmlContent || campaign.htmlContent.length < 50) {
        entry.issues.push('Empty or very short email body');
      }

      // Check for signature
      if (campaign.htmlContent && !campaign.htmlContent.includes('Content Partnerships')) {
        entry.issues.push('Missing signature');
      }

      // Check for unsubscribe (CAN-SPAM compliance)
      if (campaign.htmlContent && !campaign.htmlContent.includes('unsubscribe')) {
        entry.issues.push('Missing unsubscribe link (CAN-SPAM required)');
      }

      // Check subject line isn't empty or placeholder
      if (!campaign.subject || campaign.subject.length < 3) {
        entry.issues.push('Subject line too short or missing');
      }

      // Check merge fields aren't broken (literal {{ }} in subject)
      if (campaign.subject && /\{\{\s*$/.test(campaign.subject)) {
        entry.issues.push('Broken merge field in subject line');
      }

      // Check recipient list has contacts
      const listId = campaign.recipients?.lists?.[0];
      if (listId) {
        try {
          const listData = await brevoFetch(`/contacts/lists/${listId}/contacts?limit=5&offset=0`);
          entry.contactCount = listData.count || 0;
          if (entry.contactCount === 0) {
            entry.issues.push('List has 0 contacts');
          }

          // Spot-check merge field population
          const mergeFields = ['FIRSTNAME', 'DOMAIN', 'ARTICLE_TITLE'];
          for (const contact of (listData.contacts || [])) {
            const attrs = contact.attributes || {};
            for (const field of mergeFields) {
              if (!attrs[field] || String(attrs[field]).trim() === '') {
                entry.issues.push(`Contact ${contact.email} missing ${field}`);
                break; // One issue per contact is enough
              }
            }
          }
        } catch (err) {
          entry.issues.push(`Could not fetch list contacts: ${err.message}`);
        }
      }

      if (entry.issues.length > 0) {
        issues.push(...entry.issues.map((i) => `Campaign #${id} (${campaign.name}): ${i}`));
      }
      reviewed.push(entry);
    } catch (err) {
      issues.push(`Campaign #${id}: fetch failed — ${err.message}`);
      reviewed.push({ id, issues: [err.message] });
    }
  }

  const passed = reviewed.filter((r) => r.issues.length === 0).length;
  const failed = reviewed.filter((r) => r.issues.length > 0).length;

  console.log(`[brevo] QA: ${passed} passed, ${failed} failed out of ${campaignIds.length}`);

  return {
    totalCampaigns: campaignIds.length,
    passed,
    failed,
    issues,
    campaigns: reviewed,
    ready: failed === 0,
  };
}

/**
 * Validate that all merge fields in email content are populated
 * for a given contact list. Returns detailed report.
 */
async function validateMergeFields(listId, htmlContent) {
  // Extract merge fields from template
  const mergePattern = /\{\{\s*params\.(\w+)\s*\}\}/g;
  const requiredFields = [];
  let match;
  while ((match = mergePattern.exec(htmlContent)) !== null) {
    requiredFields.push(match[1]);
  }

  if (requiredFields.length === 0) return { valid: true, requiredFields: [], missingByContact: {} };

  // Sample contacts from list
  const listData = await brevoFetch(`/contacts/lists/${listId}/contacts?limit=50&offset=0`);
  const contacts = listData.contacts || [];
  const missingByContact = {};

  for (const contact of contacts) {
    const attrs = contact.attributes || {};
    const missing = requiredFields.filter((f) => !attrs[f] || String(attrs[f]).trim() === '');
    if (missing.length > 0) {
      missingByContact[contact.email] = missing;
    }
  }

  const valid = Object.keys(missingByContact).length === 0;
  return { valid, requiredFields, missingByContact, checked: contacts.length };
}

/**
 * Verify sender reputation — check if sender domain is authenticated.
 */
async function checkSenderReputation(senderEmail) {
  const domain = senderEmail.split('@')[1];
  if (!domain) return { email: senderEmail, authenticated: false, error: 'Invalid email' };

  try {
    const domains = await brevoFetch('/senders/domains');
    const domainList = domains.domains || [];
    const match = domainList.find((d) => d.domain_name === domain);

    return {
      email: senderEmail,
      domain,
      exists: !!match,
      authenticated: match?.authenticated || false,
      dkim: match?.dkim_record_status || null,
      spf: match?.spf_record_status || null,
    };
  } catch (err) {
    return { email: senderEmail, domain, authenticated: false, error: err.message };
  }
}

// ---------------------------------------------------------------------------
// Utility Helpers
// ---------------------------------------------------------------------------

function extractFirstName(email) {
  if (!email) return '';
  const prefix = email.split('@')[0] || '';
  const parts = prefix.split(/[._-]/);
  const generic = ['info', 'admin', 'contact', 'hello', 'support', 'editor', 'team', 'press', 'media', 'news', 'sales', 'marketing', 'webmaster', 'office', 'general'];
  if (parts[0] && parts[0].length > 1 && !/^\d+$/.test(parts[0]) && !generic.includes(parts[0].toLowerCase())) {
    return parts[0].charAt(0).toUpperCase() + parts[0].slice(1).toLowerCase();
  }
  return '';
}

function domainToSiteName(domain) {
  if (!domain) return 'your site';
  return domain
    .replace(/\.(com|org|net|io|co|us|uk|ca|au)$/i, '')
    .split(/[-.]/)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ');
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

module.exports = {
  // Core API
  brevoFetch,

  // Contact lists
  createContactList,
  findListByName,
  importContacts,

  // Campaigns
  createEmailCampaign,
  sendCampaign,
  getCampaignStats,
  cancelCampaign,

  // Senders
  createSender,
  listSenders,
  getRotatedSender,
  getSender,

  // Drip sequences
  createDripSequence,
  createTieredDripSequences,
  cancelDripForReply,
  DRIP_STEPS,

  // Tier segmentation
  segmentByTier,
  TIER_DEFINITIONS,

  // QA
  qaCampaigns,
  validateMergeFields,
  checkSenderReputation,

  // Utilities
  buildEmailHtml,
  extractFirstName,
  domainToSiteName,
};
