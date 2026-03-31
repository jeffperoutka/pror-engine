#!/usr/bin/env node
/**
 * One-time script: Seed DripQueue from existing Brevo contact lists
 * Run: node scripts/seed-drip-queue.js
 */
require('dotenv').config({ path: require('path').join(__dirname, '..', '.env.prod-check') });

const BREVO_KEY = process.env.BREVO_API_KEY;
const AIRTABLE_PAT = (process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || '').trim();
const AIRTABLE_BASE = (process.env.AIRTABLE_BASE || process.env.AIRTABLE_BASE_ID || '').trim();

// Client slug → sender config (from prospect-replenish.js CLIENT_CONFIGS)
const SENDERS = {
  'ABLE-AMMO':        { warmed: { id: 3, email: 'yam@ecom-ranker.com' }, new: { id: 28, email: 'yam@ableammo.ecom-ranker.com' } },
  'DR-DABBER':        { warmed: { id: 4, email: 'yam@ecom-guestposts.com' }, new: { id: 29, email: 'yam@drdabber.ecom-guestposts.com' } },
  'FELINA':           { warmed: { id: 5, email: 'yam@guestpost-now.com' }, new: { id: 30, email: 'yam@felina.guest-poster.com' } },
  'MILL-PACKAGING':   { warmed: { id: 6, email: 'yam@linkinsertion.live' }, new: { id: 31, email: 'yam@millpkg.linkinsertion.live' } },
  'PRIMERX':          { warmed: { id: 7, email: 'yam@guest-poster.com' }, new: { id: 32, email: 'yam@primerx.guestpost-now.com' } },
  'SMOKEA':           { warmed: { id: 8, email: 'yam@guest-post.live' }, new: { id: 33, email: 'yam@smokea.guest-post.live' } },
  'MRSKIN':           { warmed: { id: 9, email: 'yam@ecom-links.com' }, new: { id: 34, email: 'yam@mrskin.ecom-links.com' } },
  'VRAI':             { warmed: { id: 10, email: 'yam@linkinsertion.us' }, new: { id: 35, email: 'yam@vrai.linkinsertion.us' } },
  'AMS-FULFILLMENT':  { warmed: { id: 11, email: 'yam@kobo.linkinsertion.org' }, new: { id: 36, email: 'yam@amsfulfillment.ecom-ranker.com' } },
  'BUILT-BAR':        { warmed: { id: 12, email: 'yam@opensea.linkinsertion.org' }, new: { id: 37, email: 'yam@builtbar.ecom-guestposts.com' } },
  'NUTRABIO':         { warmed: { id: 2, email: 'yam@linkinsertion.org' }, new: { id: 38, email: 'yam@nutrabio.guest-poster.com' } },
  'VIVANTE-LIVING':   { warmed: { id: 3, email: 'yam@ecom-ranker.com' }, new: { id: 39, email: 'yam@vivante.linkinsertion.live' } },
  'GOODR':            { warmed: { id: 4, email: 'yam@ecom-guestposts.com' }, new: { id: 40, email: 'yam@goodr.guestpost-now.com' } },
};

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function brevoFetch(endpoint) {
  const res = await fetch(`https://api.brevo.com/v3${endpoint}`, {
    headers: { 'api-key': BREVO_KEY, 'Accept': 'application/json' },
  });
  return res.json();
}

function parseListName(name) {
  // Format: CLIENT-SLUG-W-D1-2026-03-24 or CLIENT-SLUG-N-D1-2026-03-24-replenish
  // Also: CLIENT-SLUG-OPENERS-REENGAGE-2026-03-25

  // Skip REENGAGE and OPENERS lists — those are re-engagement, not fresh prospects
  if (name.includes('REENGAGE') || name.includes('OPENERS')) return null;

  // Extract sender type from -W- or -N-
  const isWarmed = name.includes('-W-');
  const isNew = name.includes('-N-');
  if (!isWarmed && !isNew) return null;

  // Extract client slug (everything before -W- or -N-)
  const match = name.match(/^(.+?)-(W|N)-D/);
  if (!match) return null;

  return {
    clientSlug: match[1],
    senderType: isWarmed ? 'warmed' : 'new',
  };
}

async function getListContacts(listId) {
  const contacts = [];
  let offset = 0;
  while (true) {
    const data = await brevoFetch(`/contacts/lists/${listId}/contacts?limit=50&offset=${offset}`);
    if (!data.contacts || data.contacts.length === 0) break;
    contacts.push(...data.contacts);
    offset += 50;
    if (offset >= (data.count || 0)) break;
    await sleep(200);
  }
  return contacts;
}

async function createDripRecords(records) {
  let created = 0;
  for (let i = 0; i < records.length; i += 10) {
    const batch = records.slice(i, i + 10);
    const res = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE}/DripQueue`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${AIRTABLE_PAT}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ records: batch }),
    });
    const data = await res.json();
    if (data.records) {
      created += data.records.length;
    } else {
      console.error('Airtable error:', JSON.stringify(data).slice(0, 200));
    }
    await sleep(250);
  }
  return created;
}

async function main() {
  console.log('=== Seeding DripQueue from Brevo lists ===\n');

  // Get all lists
  const allLists = [];
  let offset = 0;
  while (true) {
    const data = await brevoFetch(`/contacts/lists?limit=50&offset=${offset}&sort=desc`);
    if (!data.lists || data.lists.length === 0) break;
    allLists.push(...data.lists);
    offset += 50;
    if (offset >= (data.count || 0)) break;
    await sleep(200);
  }

  console.log(`Found ${allLists.length} total lists\n`);

  // Track emails we've already queued to avoid duplicates
  const seenEmails = new Set();
  let totalQueued = 0;
  const today = new Date().toISOString().split('T')[0];
  const now = new Date().toISOString();

  for (const list of allLists) {
    const parsed = parseListName(list.name);
    if (!parsed) continue;
    if (list.uniqueSubscribers === 0) continue;

    const { clientSlug, senderType } = parsed;
    const senderConfig = SENDERS[clientSlug];
    if (!senderConfig) {
      console.log(`  Skipping ${list.name} — unknown client ${clientSlug}`);
      continue;
    }

    const sender = senderConfig[senderType];
    console.log(`Processing ${list.name} (${list.uniqueSubscribers} contacts)...`);

    const contacts = await getListContacts(list.id);
    const records = [];

    for (const c of contacts) {
      if (seenEmails.has(c.email.toLowerCase())) continue;
      seenEmails.add(c.email.toLowerCase());

      const attrs = c.attributes || {};
      records.push({
        fields: {
          Email: c.email,
          FirstName: attrs.FIRSTNAME || 'there',
          Domain: attrs.DOMAIN || c.email.split('@')[1] || '',
          SiteName: attrs.SITE_NAME || attrs.DOMAIN || '',
          ArticleTitle: attrs.ARTICLE_TITLE || '',
          ArticleUrl: attrs.ARTICLE_URL || '',
          ClientSlug: clientSlug,
          DripStep: 0,
          NextSendDate: today, // Send ASAP
          SenderEmail: sender.email,
          SenderId: sender.id,
          SenderType: senderType,
          Status: 'active',
          CreatedAt: now,
        },
      });
    }

    if (records.length > 0) {
      const created = await createDripRecords(records);
      totalQueued += created;
      console.log(`  → Queued ${created} contacts for ${clientSlug}`);
    }
  }

  console.log(`\n=== Done! Queued ${totalQueued} total contacts to DripQueue ===`);
  console.log(`These will be sent by drip-sender on its next run.`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
