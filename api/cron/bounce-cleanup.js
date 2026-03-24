const { waitUntil } = require('@vercel/functions');
const slack = require('../../shared/slack');
const { brevoFetch } = require('../../shared/brevo');

const CHANNEL = () => process.env.CHANNEL_COMMAND_CENTER;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DOMAIN_BOUNCE_THRESHOLD = 3; // bounces from same domain to flag it
const DOMAIN_BOUNCE_RATE_QUARANTINE = 0.2; // 20% bounce rate -> quarantine
const QUARANTINE_LIST_NAME = 'QUARANTINE - High Bounce Domains';
const EVENTS_PER_PAGE = 500;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function yesterday() {
  const d = new Date();
  d.setDate(d.getDate() - 1);
  d.setUTCHours(0, 0, 0, 0);
  return d.toISOString().slice(0, 10);
}

function today() {
  return new Date().toISOString().slice(0, 10);
}

function domainOf(email) {
  return (email || '').split('@')[1]?.toLowerCase() || '';
}

/**
 * Paginate through Brevo SMTP event endpoint.
 * Returns all events of a given type in the date range.
 */
async function fetchAllEvents(eventType, startDate, endDate) {
  const events = [];
  let offset = 0;
  let hasMore = true;

  while (hasMore) {
    const path = `/smtp/statistics/events?event=${eventType}&limit=${EVENTS_PER_PAGE}&offset=${offset}&startDate=${startDate}&endDate=${endDate}`;
    const data = await brevoFetch(path);
    const batch = data.events || [];
    events.push(...batch);

    if (batch.length < EVENTS_PER_PAGE) {
      hasMore = false;
    } else {
      offset += EVENTS_PER_PAGE;
    }
  }

  return events;
}

// ---------------------------------------------------------------------------
// Step 1: Pull bounce/complaint/unsubscribe events
// ---------------------------------------------------------------------------

async function pullEvents() {
  const startDate = yesterday();
  const endDate = today();

  console.log(`[bounce-cleanup] Fetching events from ${startDate} to ${endDate}`);

  const [hardBounces, complaints, unsubscribes] = await Promise.all([
    fetchAllEvents('hardBounces', startDate, endDate),
    fetchAllEvents('complaints', startDate, endDate),
    fetchAllEvents('unsubscribed', startDate, endDate),
  ]);

  console.log(`[bounce-cleanup] Found: ${hardBounces.length} bounces, ${complaints.length} complaints, ${unsubscribes.length} unsubscribes`);

  return { hardBounces, complaints, unsubscribes };
}

// ---------------------------------------------------------------------------
// Step 2: Blacklist contacts in Brevo
// ---------------------------------------------------------------------------

async function blacklistContacts(emails) {
  const results = { blacklisted: 0, alreadyBlocked: 0, errors: [] };

  for (const email of emails) {
    try {
      // Update contact with emailBlacklisted: true (keeps them for reporting)
      await brevoFetch(`/contacts/${encodeURIComponent(email)}`, {
        method: 'PUT',
        body: { emailBlacklisted: true },
      });
      results.blacklisted++;
    } catch (err) {
      if (err.status === 404) {
        // Contact doesn't exist in Brevo — create as blacklisted
        try {
          await brevoFetch('/contacts', {
            method: 'POST',
            body: { email, emailBlacklisted: true },
          });
          results.blacklisted++;
        } catch (createErr) {
          results.errors.push({ email, error: createErr.message });
        }
      } else if (err.message && /already.*blacklist/i.test(err.message)) {
        results.alreadyBlocked++;
      } else {
        results.errors.push({ email, error: err.message });
      }
    }
  }

  console.log(`[bounce-cleanup] Blacklisted ${results.blacklisted}, already blocked ${results.alreadyBlocked}, errors ${results.errors.length}`);
  return results;
}

// ---------------------------------------------------------------------------
// Step 3: Remove contacts from active campaign lists
// ---------------------------------------------------------------------------

async function cleanCampaignLists(emails) {
  if (emails.length === 0) return { listsUpdated: 0 };

  let listsUpdated = 0;

  try {
    // Get all scheduled/draft campaigns
    const campaignsData = await brevoFetch('/emailCampaigns?status=queued&limit=500&offset=0');
    const queued = campaignsData.campaigns || [];

    const draftsData = await brevoFetch('/emailCampaigns?status=draft&limit=500&offset=0');
    const drafts = draftsData.campaigns || [];

    const pendingCampaigns = [...queued, ...drafts];

    // Collect unique list IDs from pending campaigns
    const listIds = new Set();
    for (const campaign of pendingCampaigns) {
      const lists = campaign.recipients?.lists || [];
      for (const l of lists) {
        listIds.add(typeof l === 'object' ? l.id : l);
      }
    }

    // Remove blacklisted contacts from each list
    // Brevo accepts batch removal: up to 150 emails per request
    const emailBatches = [];
    for (let i = 0; i < emails.length; i += 150) {
      emailBatches.push(emails.slice(i, i + 150));
    }

    for (const listId of listIds) {
      for (const batch of emailBatches) {
        try {
          await brevoFetch(`/contacts/lists/${listId}/contacts/remove`, {
            method: 'POST',
            body: { emails: batch },
          });
        } catch (err) {
          // 404 or "not found" is fine — contact may not be on this list
          if (err.status !== 404) {
            console.warn(`[bounce-cleanup] Failed to remove contacts from list ${listId}:`, err.message);
          }
        }
      }
      listsUpdated++;
    }

    console.log(`[bounce-cleanup] Cleaned ${listsUpdated} campaign lists`);
  } catch (err) {
    console.error(`[bounce-cleanup] Error cleaning campaign lists:`, err.message);
  }

  return { listsUpdated };
}

// ---------------------------------------------------------------------------
// Step 4: Track domain-level bounce patterns
// ---------------------------------------------------------------------------

async function analyzeDomainPatterns(hardBounces, allEmails) {
  const domainBounces = {};

  // Count bounces per domain
  for (const event of hardBounces) {
    const domain = domainOf(event.email);
    if (!domain) continue;
    domainBounces[domain] = (domainBounces[domain] || 0) + 1;
  }

  // Identify high-bounce domains (3+ bounces in 24h)
  const flaggedDomains = Object.entries(domainBounces)
    .filter(([, count]) => count >= DOMAIN_BOUNCE_THRESHOLD)
    .map(([domain, bounceCount]) => ({ domain, bounceCount }));

  // For flagged domains, calculate bounce rate from all contacts we've emailed
  // Count total contacts per domain across all events
  const domainTotals = {};
  for (const email of allEmails) {
    const domain = domainOf(email);
    if (!domain) continue;
    domainTotals[domain] = (domainTotals[domain] || 0) + 1;
  }

  // Also try to get total contacts for flagged domains from Brevo
  for (const entry of flaggedDomains) {
    const total = domainTotals[entry.domain] || entry.bounceCount;
    entry.totalContacts = total;
    entry.bounceRate = total > 0 ? entry.bounceCount / total : 0;
    entry.quarantine = entry.bounceRate > DOMAIN_BOUNCE_RATE_QUARANTINE;
  }

  // Quarantine high-bounce-rate domains
  const domainsToQuarantine = flaggedDomains.filter((d) => d.quarantine);

  if (domainsToQuarantine.length > 0) {
    await quarantineDomainContacts(domainsToQuarantine.map((d) => d.domain));
  }

  return { flaggedDomains, quarantined: domainsToQuarantine.length };
}

/**
 * Move all contacts from high-bounce domains to a quarantine list.
 */
async function quarantineDomainContacts(domains) {
  if (domains.length === 0) return;

  console.log(`[bounce-cleanup] Quarantining contacts from ${domains.length} domains: ${domains.join(', ')}`);

  // Find or create quarantine list
  let quarantineList;
  try {
    quarantineList = await brevoFetch('/contacts/lists?limit=50&offset=0');
    const existing = (quarantineList.lists || []).find(
      (l) => l.name === QUARANTINE_LIST_NAME
    );

    if (existing) {
      quarantineList = existing;
    } else {
      quarantineList = await brevoFetch('/contacts/lists', {
        method: 'POST',
        body: { name: QUARANTINE_LIST_NAME },
      });
    }
  } catch (err) {
    console.error(`[bounce-cleanup] Failed to create quarantine list:`, err.message);
    return;
  }

  const listId = quarantineList.id;

  // For each domain, find contacts and add to quarantine
  for (const domain of domains) {
    try {
      // Search contacts by domain — Brevo doesn't have a direct domain search,
      // so we blacklist the domain contacts we know about from the bounce events.
      // The contacts are already being blacklisted individually in step 2.
      // Here we additionally flag them on the quarantine list for visibility.
      const searchResult = await brevoFetch(`/contacts?limit=500&offset=0&modifiedSince=${yesterday()}`);
      const contacts = (searchResult.contacts || []).filter(
        (c) => domainOf(c.email) === domain
      );

      if (contacts.length > 0) {
        const emails = contacts.map((c) => c.email);
        // Add to quarantine list in batches
        for (let i = 0; i < emails.length; i += 150) {
          const batch = emails.slice(i, i + 150);
          try {
            await brevoFetch(`/contacts/lists/${listId}/contacts/add`, {
              method: 'POST',
              body: { emails: batch },
            });
          } catch (addErr) {
            console.warn(`[bounce-cleanup] Failed to quarantine batch for ${domain}:`, addErr.message);
          }
        }
        console.log(`[bounce-cleanup] Quarantined ${contacts.length} contacts from ${domain}`);
      }
    } catch (err) {
      console.warn(`[bounce-cleanup] Error quarantining domain ${domain}:`, err.message);
    }
  }
}

// ---------------------------------------------------------------------------
// Step 5: Post Slack report
// ---------------------------------------------------------------------------

async function postReport(stats) {
  const dateStr = new Date().toLocaleDateString('en-US', {
    month: 'long',
    day: 'numeric',
  });

  let msg = `:broom: *Bounce Cleanup — ${dateStr}*\n\n`;
  msg += `Hard bounces removed: ${stats.hardBounceCount}\n`;
  msg += `Spam complaints removed: ${stats.complaintCount}\n`;
  msg += `Unsubscribes removed: ${stats.unsubscribeCount}\n`;
  msg += `Total contacts blacklisted: ${stats.totalBlacklisted}\n`;

  if (stats.flaggedDomains && stats.flaggedDomains.length > 0) {
    msg += `\n:warning: *High-bounce domains:*\n`;
    for (const d of stats.flaggedDomains) {
      const pct = (d.bounceRate * 100).toFixed(0);
      msg += `  \u2022 ${d.domain} \u2014 ${d.bounceCount}/${d.totalContacts} bounced (${pct}%)`;
      if (d.quarantine) msg += ' :octagonal_sign: quarantined';
      msg += '\n';
    }
  }

  if (stats.listsUpdated > 0) {
    msg += `\nPending campaigns updated: ${stats.listsUpdated} lists cleaned`;
  }

  if (stats.errors && stats.errors.length > 0) {
    msg += `\n\n:x: ${stats.errors.length} errors during cleanup (check logs)`;
  }

  if (stats.totalBlacklisted === 0 && (!stats.flaggedDomains || stats.flaggedDomains.length === 0)) {
    msg = `:broom: *Bounce Cleanup — ${dateStr}*\n\nAll clear — no bounces, complaints, or unsubscribes in the last 24 hours. :white_check_mark:`;
  }

  await slack.post(CHANNEL(), msg);
  console.log(`[bounce-cleanup] Slack report posted`);
}

// ---------------------------------------------------------------------------
// Main orchestrator
// ---------------------------------------------------------------------------

async function run() {
  console.log('[bounce-cleanup] Starting daily bounce cleanup...');

  try {
    // 1. Pull events
    const { hardBounces, complaints, unsubscribes } = await pullEvents();

    // Deduplicate emails across event types
    const emailSet = new Set();
    const allEventEmails = new Set();

    for (const e of hardBounces) {
      emailSet.add(e.email?.toLowerCase());
      allEventEmails.add(e.email?.toLowerCase());
    }
    for (const e of complaints) {
      emailSet.add(e.email?.toLowerCase());
      allEventEmails.add(e.email?.toLowerCase());
    }
    for (const e of unsubscribes) {
      emailSet.add(e.email?.toLowerCase());
      allEventEmails.add(e.email?.toLowerCase());
    }

    emailSet.delete(undefined);
    allEventEmails.delete(undefined);

    const uniqueEmails = [...emailSet];

    console.log(`[bounce-cleanup] ${uniqueEmails.length} unique emails to process`);

    // 2. Blacklist contacts
    const blacklistResult = await blacklistContacts(uniqueEmails);

    // 3. Clean campaign lists
    const cleanResult = await cleanCampaignLists(uniqueEmails);

    // 4. Analyze domain patterns (hard bounces only)
    const domainResult = await analyzeDomainPatterns(hardBounces, [...allEventEmails]);

    // 5. Post Slack report
    const stats = {
      hardBounceCount: hardBounces.length,
      complaintCount: complaints.length,
      unsubscribeCount: unsubscribes.length,
      totalBlacklisted: blacklistResult.blacklisted,
      listsUpdated: cleanResult.listsUpdated,
      flaggedDomains: domainResult.flaggedDomains,
      errors: blacklistResult.errors,
    };

    await postReport(stats);

    console.log('[bounce-cleanup] Done.');
    return stats;
  } catch (err) {
    console.error('[bounce-cleanup] Fatal error:', err);

    // Still try to notify Slack
    try {
      await slack.post(
        CHANNEL(),
        `:rotating_light: *Bounce Cleanup Failed*\n\n\`\`\`${err.message}\`\`\`\n\nCheck Vercel logs for details.`
      );
    } catch (slackErr) {
      console.error('[bounce-cleanup] Could not post error to Slack:', slackErr.message);
    }

    throw err;
  }
}

// ---------------------------------------------------------------------------
// Vercel serverless export
// ---------------------------------------------------------------------------

module.exports = async (req, res) => {
  waitUntil(run());
  res.status(200).json({ ok: true });
};
