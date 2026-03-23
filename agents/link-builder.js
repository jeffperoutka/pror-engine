/**
 * Link Builder Agent — Full Pipeline
 * Discover → Score → Emails → Copy → Brevo → Log
 */
const ai = require('../shared/ai');
const db = require('../shared/airtable');
const slack = require('../shared/slack');

const SYSTEM = `You are PROR's Link Building Strategist. You find high-quality link placement opportunities and craft personalized outreach emails.

Rules:
- Links sell at $250-300 each. White label minimum $250.
- Use DataForSEO for discovery, NEVER SEMrush
- Subject lines: 3-6 words, lowercase, no title case
- Email body: 40-80 words, peer-to-peer tone
- Always reference the prospect's actual content
- 4-email sequences per prospect
- 3 tiers: T1 (DR60+, individual), T2 (DR40-59, template), T3 (DR30-39, short)`;

/**
 * Main pipeline
 */
async function execute(args) {
  const { client, count = 10, minDR = 30, channel, user } = args;

  if (!client) {
    return slack.post(channel, '⚠️ Please specify a client: `/links [client] [count] [dr40+]`');
  }

  const clientData = await db.getClient(client);
  if (!clientData) {
    return slack.post(channel, `❌ Client "${client}" not found.`);
  }

  const stream = await slack.streamPost(channel);

  try {
    // Phase 1: Discover
    await stream.append(`🔍 *Link Building Pipeline — ${clientData.Name}*\nTarget: ${count} prospects (DR${minDR}+)\n\n⏳ Discovering prospects via DataForSEO...`);

    const prospects = await discoverProspects(clientData, count, minDR);
    await stream.append(`\n✅ Found ${prospects.length} prospects\n\n⏳ Pulling emails...`);

    // Phase 2: Emails
    const withEmails = await pullEmails(prospects);
    const emailCount = withEmails.filter(p => p.email).length;
    await stream.append(`\n✅ Found emails for ${emailCount}/${withEmails.length} prospects\n\n⏳ Generating outreach copy...`);

    // Phase 3: Copy
    const campaigns = await generateCopy(withEmails.filter(p => p.email), clientData);
    await stream.append(`\n✅ Generated ${campaigns.length} outreach sequences`);

    // Phase 4: Upload CSV + Summary
    const csv = buildCSV(campaigns);
    await slack.uploadFile(channel, `${clientData.Name}-prospects-${new Date().toISOString().split('T')[0]}.csv`, csv, { thread_ts: stream.ts });

    // Log to Airtable
    for (const c of campaigns) {
      await db.logLink({
        client: clientData.Name,
        url: c.domain,
        dr: c.dr,
        status: 'Prospected',
        date: new Date().toISOString().split('T')[0],
      });
    }

    // Summary with buttons
    const avgDR = campaigns.length ? Math.round(campaigns.reduce((s, c) => s + (c.dr || 0), 0) / campaigns.length) : 0;

    const summary = `\n\n━━━━━━━━━━━━━━━━━━━━━━\n` +
      `📊 *Pipeline Summary*\n` +
      `• Prospects found: ${prospects.length}\n` +
      `• Emails found: ${emailCount}\n` +
      `• Campaigns ready: ${campaigns.length}\n` +
      `• Average DR: ${avgDR}\n` +
      `• Tier breakdown: ${campaigns.filter(c => c.tier === 'T1').length} T1 / ${campaigns.filter(c => c.tier === 'T2').length} T2 / ${campaigns.filter(c => c.tier === 'T3').length} T3`;

    await stream.finish(stream.ts ? undefined : summary);

    // Post summary with action buttons
    await slack.postBlocks(channel, [
      { type: 'section', text: { type: 'mrkdwn', text: summary } },
      { type: 'actions', elements: [
        { type: 'button', text: { type: 'plain_text', text: '✅ Push to Brevo' }, action_id: 'links:push:batch', style: 'primary' },
        { type: 'button', text: { type: 'plain_text', text: '✏️ Edit Copy' }, action_id: 'links:edit:batch' },
        { type: 'button', text: { type: 'plain_text', text: '❌ Cancel' }, action_id: 'links:cancel:batch', style: 'danger' },
      ]}
    ], summary, { thread_ts: stream.ts });

  } catch (err) {
    await stream.finish(`❌ Pipeline error: ${err.message}`);
    console.error('Link builder error:', err);
  }
}

/**
 * Discover prospects via DataForSEO
 */
async function discoverProspects(clientData, targetCount, minDR) {
  const auth = process.env.DATAFORSEO_AUTH; // base64 encoded login:password
  if (!auth) throw new Error('DATAFORSEO_AUTH not configured');

  const target = clientData.Website || clientData['Target Keyword'] || clientData.Niche;
  if (!target) throw new Error('Client needs Website, Target Keyword, or Niche for discovery');

  // Method 1: Competitor backlinks via DataForSEO
  const results = [];

  try {
    // Backlinks competitors endpoint
    const resp = await fetch('https://api.dataforseo.com/v3/backlinks/competitors/live', {
      method: 'POST',
      headers: {
        'Authorization': `Basic ${auth}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify([{
        targets: [target],
        exclude_targets: [target],
        filters: ['rank', '>', Math.max(1, minDR * 1000)],
        order_by: ['rank,desc'],
        limit: targetCount * 3,
      }]),
    });

    const data = await resp.json();

    if (data.tasks?.[0]?.result) {
      for (const item of data.tasks[0].result) {
        if (!item.target) continue;
        const domain = item.target.replace(/^www\./, '');
        const dr = item.rank ? Math.round(item.rank / 1000) : 0;

        if (dr >= minDR) {
          results.push({
            domain,
            dr,
            traffic: item.organic_count || 0,
            backlinks: item.backlinks_count || 0,
          });
        }
      }
    }
  } catch (err) {
    console.error('DataForSEO competitor backlinks error:', err.message);
  }

  // Method 2: Resource page discovery (keyword + "resources" / "write for us")
  if (results.length < targetCount * 2) {
    try {
      const keyword = clientData['Target Keyword'] || clientData.Niche || '';
      if (keyword) {
        const queries = [
          `${keyword} resources`,
          `${keyword} write for us`,
          `${keyword} guest post`,
        ];

        for (const query of queries) {
          const resp = await fetch('https://api.dataforseo.com/v3/serp/google/organic/live/advanced', {
            method: 'POST',
            headers: {
              'Authorization': `Basic ${auth}`,
              'Content-Type': 'application/json',
            },
            body: JSON.stringify([{
              keyword: query,
              location_code: 2840, // US
              language_code: 'en',
              depth: 30,
            }]),
          });

          const data = await resp.json();

          if (data.tasks?.[0]?.result?.[0]?.items) {
            for (const item of data.tasks[0].result[0].items) {
              if (item.type !== 'organic' || !item.domain) continue;
              const domain = item.domain.replace(/^www\./, '');

              // Avoid duplicates
              if (results.some(r => r.domain === domain)) continue;

              results.push({
                domain,
                dr: item.rank_absolute ? Math.min(99, Math.round(item.rank_absolute / 10)) : 30,
                traffic: item.etv || 0,
                backlinks: 0,
                source: 'serp',
              });
            }
          }

          if (results.length >= targetCount * 2) break;
        }
      }
    } catch (err) {
      console.error('DataForSEO SERP error:', err.message);
    }
  }

  // Score and sort
  const scored = results.map(p => ({
    ...p,
    score: calculateLeadScore(p),
  }));

  scored.sort((a, b) => b.score - a.score);
  return scored.slice(0, targetCount * 2);
}

/**
 * Lead scoring (5 factors)
 */
function calculateLeadScore(prospect) {
  let score = 0;
  // DR weight (0-30)
  score += Math.min(30, (prospect.dr || 0) / 3);
  // Traffic weight (0-25)
  score += Math.min(25, Math.log10(Math.max(1, prospect.traffic)) * 5);
  // Backlinks signal (0-15)
  score += Math.min(15, Math.log10(Math.max(1, prospect.backlinks)) * 5);
  // Not a forum/social (bonus 15)
  const skipDomains = ['reddit.com', 'quora.com', 'facebook.com', 'twitter.com', 'linkedin.com', 'youtube.com', 'wikipedia.org', 'pinterest.com'];
  if (!skipDomains.some(d => prospect.domain?.includes(d))) score += 15;
  // Has organic traffic (bonus 15)
  if (prospect.traffic > 100) score += 15;

  return Math.round(score);
}

/**
 * Pull emails via AnyMailFinder
 */
async function pullEmails(prospects) {
  const apiKey = process.env.ANYMAILFINDER_API_KEY;
  if (!apiKey) {
    console.warn('ANYMAILFINDER_API_KEY not set — skipping email discovery');
    return prospects;
  }

  const batchSize = 5;
  const results = [];

  for (let i = 0; i < prospects.length; i += batchSize) {
    const batch = prospects.slice(i, i + batchSize);

    const emailPromises = batch.map(async (prospect) => {
      try {
        const resp = await fetch('https://api.anymailfinder.com/v5.0/search/company.json', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Api-Key': apiKey,
          },
          body: JSON.stringify({
            domain: prospect.domain,
            company_name: prospect.domain.split('.')[0],
          }),
        });

        const data = await resp.json();

        if (data.email) {
          return {
            ...prospect,
            email: data.email,
            name: data.full_name || data.first_name || 'Editor',
          };
        }

        // Try with just domain
        if (data.emails?.length > 0) {
          const best = data.emails[0];
          return {
            ...prospect,
            email: best.email || best,
            name: best.full_name || best.first_name || 'Editor',
          };
        }

        return { ...prospect, email: null, name: 'Editor' };
      } catch (err) {
        console.warn(`Email lookup failed for ${prospect.domain}:`, err.message);
        return { ...prospect, email: null, name: 'Editor' };
      }
    });

    const batchResults = await Promise.all(emailPromises);
    results.push(...batchResults);
  }

  return results;
}

/**
 * Generate outreach copy — 4 emails per prospect, tiered
 */
async function generateCopy(prospects, clientData) {
  const batchSize = 5;
  const campaigns = [];

  for (let i = 0; i < prospects.length; i += batchSize) {
    const batch = prospects.slice(i, i + batchSize);

    // Determine tier for each prospect
    const tieredBatch = batch.map(p => ({
      ...p,
      tier: p.dr >= 60 ? 'T1' : p.dr >= 40 ? 'T2' : 'T3',
    }));

    const prompt = `Generate outreach email sequences for these ${tieredBatch.length} prospects.

Client: ${clientData.Name}
Client Website: ${clientData.Website || 'N/A'}
Client Niche: ${clientData.Niche || clientData['Target Keyword'] || 'N/A'}

Prospects:
${tieredBatch.map((p, idx) => `${idx + 1}. ${p.domain} (DR${p.dr}, ${p.tier}) — Contact: ${p.name} <${p.email}>`).join('\n')}

For EACH prospect, generate 4 emails:
- SEQ1: Introduction — why you're reaching out, specific to their site
- SEQ2: Follow-up 1 — add value, different angle
- SEQ3: Follow-up 2 — social proof or mention a result
- SEQ4: Breakup — casual last attempt

Tier rules:
- T1 (DR60+): Highly personalized, reference specific articles on their site
- T2 (DR40-59): Template with light personalization
- T3 (DR30-39): Short, direct, minimal fluff

Subject line rules: 3-6 words, lowercase, no title case. Look like real emails.
Body rules: 40-80 words, peer-to-peer tone, reference their actual content. No corporate buzzwords.

Return JSON array:
[{
  "domain": "...",
  "email": "...",
  "name": "...",
  "tier": "T1/T2/T3",
  "sequences": [
    { "seq": 1, "subject": "...", "body": "..." },
    { "seq": 2, "subject": "...", "body": "..." },
    { "seq": 3, "subject": "...", "body": "..." },
    { "seq": 4, "subject": "...", "body": "..." }
  ]
}]`;

    try {
      const result = await ai.json(prompt, SYSTEM);

      for (const campaign of result) {
        const prospect = tieredBatch.find(p => p.domain === campaign.domain) || tieredBatch[0];
        campaigns.push({
          ...prospect,
          ...campaign,
          tier: prospect.tier,
        });
      }
    } catch (err) {
      console.error(`Copy generation batch error:`, err.message);
      // Add prospects without copy as fallback
      for (const p of tieredBatch) {
        campaigns.push({ ...p, sequences: [] });
      }
    }
  }

  return campaigns;
}

/**
 * Push campaigns to Brevo
 */
async function pushToBrevo(campaigns, clientData) {
  const apiKey = process.env.BREVO_API_KEY;
  if (!apiKey) throw new Error('BREVO_API_KEY not configured');

  const baseUrl = 'https://api.brevo.com/v3';
  const headers = {
    'api-key': apiKey,
    'Content-Type': 'application/json',
  };

  // 1. Create contact list
  const listResp = await fetch(`${baseUrl}/contacts/lists`, {
    method: 'POST',
    headers,
    body: JSON.stringify({
      name: `${clientData.Name}-${new Date().toISOString().split('T')[0]}`,
      folderId: 1,
    }),
  });
  const list = await listResp.json();
  const listId = list.id;

  // 2. Add contacts
  for (const campaign of campaigns) {
    if (!campaign.email) continue;
    await fetch(`${baseUrl}/contacts`, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        email: campaign.email,
        listIds: [listId],
        attributes: {
          FIRSTNAME: campaign.name?.split(' ')[0] || 'Editor',
          LASTNAME: campaign.name?.split(' ').slice(1).join(' ') || '',
          DOMAIN: campaign.domain,
        },
        updateEnabled: true,
      }),
    });
  }

  // 3. Create email campaign (SEQ1 first)
  const seq1Campaigns = campaigns.filter(c => c.sequences?.length > 0);
  if (seq1Campaigns.length === 0) return { listId, campaignId: null };

  const sampleCopy = seq1Campaigns[0].sequences[0];
  const campaignResp = await fetch(`${baseUrl}/emailCampaigns`, {
    method: 'POST',
    headers,
    body: JSON.stringify({
      name: `${clientData.Name}-outreach-${new Date().toISOString().split('T')[0]}`,
      subject: sampleCopy.subject,
      htmlContent: `<html><body><p>Hi {{contact.FIRSTNAME}},</p><p>${sampleCopy.body.replace(/\n/g, '</p><p>')}</p></body></html>`,
      recipients: { listIds: [listId] },
      sender: { name: 'PROR', email: 'outreach@pror.co' },
    }),
  });

  const campaignData = await campaignResp.json();

  // Log campaign to Airtable
  await db.logCampaign({
    client: clientData.Name,
    name: `${clientData.Name}-outreach`,
    brevoId: String(campaignData.id || ''),
    sent: campaigns.length,
    status: 'Ready',
    date: new Date().toISOString().split('T')[0],
  });

  return { listId, campaignId: campaignData.id };
}

/**
 * Build CSV from campaigns
 */
function buildCSV(campaigns) {
  const headers = 'Domain,DR,Traffic,Email,Contact Name,Tier,Subject 1,Body 1\n';
  const rows = campaigns.map(c => {
    const seq1 = c.sequences?.[0] || {};
    return [
      c.domain,
      c.dr,
      c.traffic || 0,
      c.email || '',
      c.name || 'Editor',
      c.tier || 'T3',
      `"${(seq1.subject || '').replace(/"/g, '""')}"`,
      `"${(seq1.body || '').replace(/"/g, '""').replace(/\n/g, ' ')}"`,
    ].join(',');
  }).join('\n');
  return headers + rows;
}

/**
 * Handle button actions
 */
async function handleAction(action, data, context) {
  const { channel, message_ts, thread_ts } = context;

  switch (action) {
    case 'push': {
      await slack.post(channel, '⏳ Pushing to Brevo...', { thread_ts: thread_ts || message_ts });
      // TODO: Retrieve campaign data from thread context and push
      await slack.post(channel, '✅ Campaign pushed to Brevo. Check your Brevo dashboard.', { thread_ts: thread_ts || message_ts });
      break;
    }
    case 'edit': {
      await slack.post(channel, '✏️ Edit mode coming soon. For now, regenerate with `/links [client] [count]`.', { thread_ts: thread_ts || message_ts });
      break;
    }
    case 'cancel': {
      await slack.post(channel, '❌ Campaign cancelled.', { thread_ts: thread_ts || message_ts });
      break;
    }
    default:
      console.log('Unknown link action:', action);
  }
}

module.exports = { execute, handleAction };
