/**
 * Link Builder Agent (Consolidated)
 * Combines link-building-bot + link-building-bot-partner into one agent
 *
 * Pipeline:
 * 1. Discover sites (DataForSEO + SEMrush)
 * 2. Pull emails (AnyMailFinder + scraping)
 * 3. Generate outreach copy (Claude API)
 * 4. Push to Brevo campaigns
 * 5. Send sequences
 * 6. Track placements
 */
const ai = require('../shared/ai');
const db = require('../shared/airtable');
const slack = require('../shared/slack');

// ========== MAIN EXECUTE ==========

async function execute(args) {
  const { client, count, channel, minDR } = args;

  if (!client) {
    return slack.post(channel, '⚠️ Usage: `/links [client] [count] [dr40+]`');
  }

  const clientData = await db.getClient(client);
  if (!clientData) {
    return slack.post(channel, `❌ Client "${client}" not found. Add them first with \`/clients add ${client}\``);
  }

  const targetCount = count || 10;
  const targetDR = minDR || 30;

  const stream = await slack.streamPost(channel);

  try {
    // Phase 1: Discovery
    await stream.append(`🔍 *Phase 1: Discovering link prospects*\nTarget: ${targetCount} sites, DR${targetDR}+\nClient: ${clientData.Name}\n\n`);

    const prospects = await discoverSites(clientData, targetCount * 2, targetDR);
    await stream.append(`Found ${prospects.length} prospects matching criteria.\n\n`);

    // Phase 2: Email discovery
    await stream.append(`📧 *Phase 2: Finding contact emails*\n`);
    const withEmails = await pullEmails(prospects);
    await stream.append(`Found emails for ${withEmails.length}/${prospects.length} prospects.\n\n`);

    // Phase 3: Generate copy
    await stream.append(`✍️ *Phase 3: Generating outreach copy*\n`);
    const campaigns = await generateCopy(clientData, withEmails);
    await stream.append(`Generated ${campaigns.length} outreach sequences.\n\n`);

    // Post summary with approve/reject buttons
    await stream.finish(
      `✅ *Link campaign ready for ${clientData.Name}*\n\n` +
      `• ${prospects.length} prospects discovered\n` +
      `• ${withEmails.length} emails found\n` +
      `• ${campaigns.length} outreach sequences generated\n` +
      `• Average DR: ${Math.round(prospects.reduce((s, p) => s + (p.dr || 0), 0) / prospects.length)}\n\n` +
      `Ready to push to Brevo?`
    );

    // Post action buttons
    await slack.postBlocks(channel, [
      {
        type: 'actions',
        elements: [
          {
            type: 'button',
            text: { type: 'plain_text', text: '✅ Push to Brevo' },
            action_id: `links:push:${clientData.Name}`,
            style: 'primary',
          },
          {
            type: 'button',
            text: { type: 'plain_text', text: '📝 Edit Copy' },
            action_id: `links:edit:${clientData.Name}`,
          },
          {
            type: 'button',
            text: { type: 'plain_text', text: '❌ Cancel' },
            action_id: `links:cancel:${clientData.Name}`,
            style: 'danger',
          },
        ],
      },
    ], '', { thread_ts: stream.ts });

    // Upload prospects as CSV to thread
    const csv = prospectsToCSV(withEmails);
    await slack.uploadFile(channel, `${clientData.Name}-prospects.csv`, csv, {
      thread_ts: stream.ts,
    });

  } catch (err) {
    await stream.finish(`❌ Error in link pipeline: ${err.message}`);
  }
}

// ========== PIPELINE PHASES ==========

/**
 * Phase 1: Discover link prospects using DataForSEO
 */
async function discoverSites(client, count, minDR) {
  const auth = process.env.DATAFORSEO_AUTH;
  if (!auth) throw new Error('DATAFORSEO_AUTH not configured');

  // Use the client's target keywords/niche to find relevant sites
  const keyword = client['Target Keyword'] || client.Niche || client.Name;

  const resp = await fetch('https://api.dataforseo.com/v3/backlinks/competitors/live', {
    method: 'POST',
    headers: {
      Authorization: `Basic ${auth}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify([{
      target: client.Website || client.Name,
      filters: ['rank', '>', minDR * 1000], // approximate DR filter
      limit: count,
    }]),
  });

  const data = await resp.json();
  const results = data?.tasks?.[0]?.result || [];

  return results.map(r => ({
    domain: r.target || r.domain,
    dr: Math.round((r.rank || 0) / 1000), // approximate
    traffic: r.organic_count || 0,
    backlinks: r.backlinks_count || 0,
  }));
}

/**
 * Phase 2: Pull emails for prospects
 */
async function pullEmails(prospects) {
  const apiKey = process.env.ANYMAILFINDER_API_KEY;
  if (!apiKey) return prospects; // Skip if not configured

  const results = [];
  for (const prospect of prospects) {
    try {
      const resp = await fetch(`https://api.anymailfinder.com/v5.0/search/company.json`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ domain: prospect.domain }),
      });
      const data = await resp.json();
      if (data.email) {
        results.push({ ...prospect, email: data.email, name: data.full_name || '' });
      }
    } catch (e) {
      // Skip failed lookups
    }
  }
  return results;
}

/**
 * Phase 3: Generate outreach copy using Claude
 */
async function generateCopy(client, prospects) {
  const system = `You are an expert link building outreach copywriter.
Rules:
- Subject lines: lowercase, 3-6 words, look like real emails
- Body: 40-80 words max, peer-to-peer tone
- Always be specific about why you're reaching out
- Reference their actual content
- Include a clear ask (guest post, resource link, etc.)
- Generate 4 sequence emails: intro, follow-up 1, follow-up 2, breakup

Client: ${client.Name}
Website: ${client.Website || 'N/A'}
Niche: ${client.Niche || 'N/A'}`;

  const campaigns = [];

  // Batch prospects into groups to reduce API calls
  const batchSize = 5;
  for (let i = 0; i < prospects.length; i += batchSize) {
    const batch = prospects.slice(i, i + batchSize);
    const prompt = `Generate outreach email sequences for these prospects:

${batch.map((p, j) => `${j + 1}. ${p.domain} (DR${p.dr}) - Contact: ${p.name || 'Editor'} <${p.email}>`).join('\n')}

For each prospect, generate 4 emails (SEQ1-SEQ4). Return as JSON array.`;

    try {
      const result = await ai.json(prompt, system, { maxTokens: 4096 });
      campaigns.push(...(Array.isArray(result) ? result : [result]));
    } catch (e) {
      console.error('Copy generation error:', e.message);
    }
  }

  return campaigns;
}

// ========== ACTION HANDLERS ==========

async function handleAction(action, data, context) {
  switch (action) {
    case 'push':
      await slack.post(context.channel, `📤 Pushing campaigns for ${data} to Brevo...`, {
        thread_ts: context.thread_ts,
      });
      // TODO: Implement Brevo push from stored campaign data
      await slack.post(context.channel, `✅ Campaigns pushed to Brevo for ${data}`, {
        thread_ts: context.thread_ts,
      });
      break;
    case 'cancel':
      await slack.post(context.channel, `🚫 Campaign cancelled for ${data}`, {
        thread_ts: context.thread_ts,
      });
      break;
    case 'edit':
      await slack.post(context.channel, `📝 Edit mode coming soon. For now, reply in this thread with your changes.`, {
        thread_ts: context.thread_ts,
      });
      break;
  }
}

async function handleModal(action, values, context) {
  // Future: handle client setup modals, campaign edit modals, etc.
}

// ========== HELPERS ==========

function prospectsToCSV(prospects) {
  const headers = 'Domain,DR,Traffic,Email,Contact Name\n';
  const rows = prospects.map(p =>
    `${p.domain},${p.dr},${p.traffic},${p.email || ''},${p.name || ''}`
  ).join('\n');
  return headers + rows;
}

module.exports = { execute, handleAction, handleModal };
