/**
 * Link Builder Agent — Full Pipeline
 * Discover -> Score -> Emails -> Copy -> Brevo Drip Sequences -> Log
 *
 * Brevo integration uses shared/brevo.js for:
 * - Tier-segmented 4-step drip sequences (T1/T2/T3)
 * - Campaign QA with Slack approval before sending
 * - Merge field personalization per prospect
 * - Sender rotation across available domains
 * - Financial logging of campaign costs + expected revenue
 */
const ai = require('../shared/ai');
const db = require('../shared/airtable');
const slack = require('../shared/slack');
const brevo = require('../shared/brevo');

const SYSTEM = `You are PROR's Link Building Strategist. You find high-quality link placement opportunities and craft personalized outreach emails.

Rules:
- Links sell at $250-300 each. White label minimum $250.
- Use DataForSEO for discovery, NEVER SEMrush
- Subject lines: 3-6 words, lowercase, no title case
- Email body: 40-80 words, peer-to-peer tone
- Always reference the prospect's actual content
- 4-email sequences per prospect
- 3 tiers: T1 (DR60+, individual), T2 (DR40-59, template), T3 (DR30-39, short)`;

// Cost assumptions for financial logging
const BREVO_COST_PER_EMAIL = 0.004; // ~$4 per 1000 emails
const EXPECTED_REPLY_RATE = { T1: 0.12, T2: 0.08, T3: 0.05 };
const EXPECTED_CONVERSION_RATE = { T1: 0.40, T2: 0.25, T3: 0.15 };
const LINK_PRICE = 275; // avg revenue per placed link

/**
 * Main pipeline
 */
async function execute(args) {
  const { client, count = 10, minDR = 30, channel, user } = args;

  if (!client) {
    return slack.post(channel, 'Please specify a client: `/links [client] [count] [dr40+]`');
  }

  const clientData = await db.getClient(client);
  if (!clientData) {
    return slack.post(channel, `Client "${client}" not found.`);
  }

  const stream = await slack.streamPost(channel);

  try {
    // Phase 1: Discover
    await stream.append(`*Link Building Pipeline -- ${clientData.Name}*\nTarget: ${count} prospects (DR${minDR}+)\n\nDiscovering prospects via DataForSEO...`);

    const prospects = await discoverProspects(clientData, count, minDR);
    await stream.append(`\nFound ${prospects.length} prospects\n\nPulling emails...`);

    // Phase 2: Emails
    const withEmails = await pullEmails(prospects);
    const emailCount = withEmails.filter(p => p.email).length;
    await stream.append(`\nFound emails for ${emailCount}/${withEmails.length} prospects\n\nGenerating outreach copy...`);

    // Phase 3: Copy
    const campaigns = await generateCopy(withEmails.filter(p => p.email), clientData);
    await stream.append(`\nGenerated ${campaigns.length} outreach sequences`);

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
    const tierBreakdown = {
      T1: campaigns.filter(c => c.tier === 'T1'),
      T2: campaigns.filter(c => c.tier === 'T2'),
      T3: campaigns.filter(c => c.tier === 'T3'),
    };

    const summary = `\n\n--------------------\n` +
      `*Pipeline Summary*\n` +
      `- Prospects found: ${prospects.length}\n` +
      `- Emails found: ${emailCount}\n` +
      `- Campaigns ready: ${campaigns.length}\n` +
      `- Average DR: ${avgDR}\n` +
      `- Tier breakdown: ${tierBreakdown.T1.length} T1 / ${tierBreakdown.T2.length} T2 / ${tierBreakdown.T3.length} T3\n` +
      `- Est. total emails: ${campaigns.length * 4} (4-step drip x ${campaigns.length} prospects)`;

    await stream.finish(stream.ts ? undefined : summary);

    // Store campaign data in thread for push action
    // Post summary with action buttons
    await slack.postBlocks(channel, [
      { type: 'section', text: { type: 'mrkdwn', text: summary } },
      { type: 'actions', elements: [
        { type: 'button', text: { type: 'plain_text', text: 'Push to Brevo (Drip)' }, action_id: 'links:push:batch', style: 'primary', value: JSON.stringify({ clientName: clientData.Name }) },
        { type: 'button', text: { type: 'plain_text', text: 'Edit Copy' }, action_id: 'links:edit:batch' },
        { type: 'button', text: { type: 'plain_text', text: 'Cancel' }, action_id: 'links:cancel:batch', style: 'danger' },
      ]}
    ], summary, { thread_ts: stream.ts });

    // Cache campaign data for the push action
    _lastCampaignCache.set(clientData.Name, { campaigns, clientData, threadTs: stream.ts, channel });

  } catch (err) {
    await stream.finish(`Pipeline error: ${err.message}`);
    console.error('Link builder error:', err);
  }
}

// In-memory cache for campaign data between discover and push actions
const _lastCampaignCache = new Map();

/**
 * Push campaigns to Brevo as tier-segmented 4-step drip sequences.
 *
 * Flow:
 * 1. Segment prospects into T1/T2/T3
 * 2. Create separate drip sequences per tier
 * 3. Run campaign QA (merge fields, content, sender)
 * 4. Post QA preview to Slack for approval
 * 5. Log financial data (campaign costs + expected revenue)
 */
async function pushToBrevo(campaigns, clientData, context = {}) {
  const { channel, thread_ts } = context;
  const dateStr = new Date().toISOString().split('T')[0];
  const slug = clientData.Name.toLowerCase().replace(/\s+/g, '-');

  if (channel) {
    await slack.post(channel, 'Pushing to Brevo with 4-step drip sequences...', { thread_ts });
  }

  // 1. Segment campaigns by tier
  const segments = brevo.segmentByTier(campaigns);
  const dripResults = {};
  const allCampaignIds = [];

  // 2. Get rotated sender for domain reputation protection
  let sender;
  try {
    sender = await brevo.getSender();
  } catch (err) {
    const msg = `Brevo sender error: ${err.message}. Configure BREVO_SENDER_ID or create a sender.`;
    if (channel) await slack.post(channel, msg, { thread_ts });
    throw new Error(msg);
  }

  // Check sender reputation before proceeding
  const reputation = await brevo.checkSenderReputation(sender.email);
  if (!reputation.authenticated) {
    const warning = `Warning: Sender domain ${reputation.domain} is not authenticated (DKIM: ${reputation.dkim}, SPF: ${reputation.spf}). Emails may land in spam.`;
    console.warn(`[link-builder] ${warning}`);
    if (channel) await slack.post(channel, warning, { thread_ts });
  }

  // 3. Create drip sequence per tier
  for (const [tier, tierCampaigns] of Object.entries(segments)) {
    if (tierCampaigns.length === 0) continue;

    if (channel) {
      await slack.post(channel, `Creating ${tier} drip sequence for ${tierCampaigns.length} prospects...`, { thread_ts });
    }

    // Build email copy for this tier's drip steps
    // Use the first campaign's sequences as template (AI already generated tier-appropriate copy)
    const withCopy = tierCampaigns.filter(c => c.sequences?.length >= 4);
    if (withCopy.length === 0) {
      console.warn(`[link-builder] No complete sequences for ${tier} — skipping`);
      continue;
    }

    // For T1: each prospect gets individual drip (bespoke copy)
    // For T2/T3: all prospects share template copy
    if (tier === 'T1') {
      // Individual drip per T1 prospect
      const t1Results = [];
      for (const campaign of withCopy) {
        const sequences = {
          subject1: campaign.sequences[0]?.subject || '',
          body1: campaign.sequences[0]?.body || '',
          subject2: campaign.sequences[1]?.subject || '',
          body2: campaign.sequences[1]?.body || '',
          subject3: campaign.sequences[2]?.subject || '',
          body3: campaign.sequences[2]?.body || '',
          subject4: campaign.sequences[3]?.subject || '',
          body4: campaign.sequences[3]?.body || '',
        };

        try {
          const result = await brevo.createDripSequence(
            { name: clientData.Name, slug },
            tier,
            [campaign], // single prospect
            sequences,
            { sender }
          );
          t1Results.push(result);
          if (result.campaigns) {
            allCampaignIds.push(...result.campaigns.filter(c => c.campaignId).map(c => c.campaignId));
          }
        } catch (err) {
          console.error(`[link-builder] T1 drip failed for ${campaign.domain}:`, err.message);
        }
      }
      dripResults[tier] = t1Results;
    } else {
      // Template drip for T2/T3 — use first prospect's copy as shared template
      const templateCampaign = withCopy[0];
      const sequences = {
        subject1: templateCampaign.sequences[0]?.subject || '',
        body1: templateCampaign.sequences[0]?.body || '',
        subject2: templateCampaign.sequences[1]?.subject || '',
        body2: templateCampaign.sequences[1]?.body || '',
        subject3: templateCampaign.sequences[2]?.subject || '',
        body3: templateCampaign.sequences[2]?.body || '',
        subject4: templateCampaign.sequences[3]?.subject || '',
        body4: templateCampaign.sequences[3]?.body || '',
      };

      try {
        const result = await brevo.createDripSequence(
          { name: clientData.Name, slug },
          tier,
          tierCampaigns, // all prospects in this tier
          sequences,
          { sender }
        );
        dripResults[tier] = result;
        if (result.campaigns) {
          allCampaignIds.push(...result.campaigns.filter(c => c.campaignId).map(c => c.campaignId));
        }
      } catch (err) {
        console.error(`[link-builder] ${tier} drip failed:`, err.message);
        if (channel) await slack.post(channel, `${tier} drip creation failed: ${err.message}`, { thread_ts });
      }
    }
  }

  // 4. Campaign QA — validate all campaigns before Slack approval
  let qaReport = null;
  if (allCampaignIds.length > 0) {
    if (channel) {
      await slack.post(channel, `Running QA on ${allCampaignIds.length} campaigns...`, { thread_ts });
    }

    qaReport = await brevo.qaCampaigns(allCampaignIds);
  }

  // 5. Post QA preview to Slack for approval
  if (channel && qaReport) {
    const qaBlocks = buildQAPreviewBlocks(clientData, dripResults, qaReport, campaigns.length);
    await slack.postBlocks(channel, qaBlocks, 'Campaign QA Report', { thread_ts });
  }

  // 6. Financial logging — campaign costs and expected revenue
  await logCampaignFinancials(clientData, campaigns, dripResults);

  // 7. Log campaigns to Airtable
  for (const [tier, result] of Object.entries(dripResults)) {
    const campaignList = Array.isArray(result) ? result : [result];
    for (const r of campaignList) {
      const campaignIds = (r.campaigns || []).filter(c => c.campaignId).map(c => String(c.campaignId)).join(',');
      await db.logCampaign({
        client: clientData.Name,
        name: `${slug}-${tier}-drip-${dateStr}`,
        brevoId: campaignIds,
        sent: r.contactCount || 0,
        status: qaReport?.ready ? 'QA Passed' : 'QA Failed',
        date: dateStr,
      });
    }
  }

  return { dripResults, qaReport, campaignIds: allCampaignIds };
}

/**
 * Build Slack blocks for QA preview / approval.
 */
function buildQAPreviewBlocks(clientData, dripResults, qaReport, totalProspects) {
  const blocks = [];

  // Header
  blocks.push({
    type: 'header',
    text: { type: 'plain_text', text: `Brevo QA Report -- ${clientData.Name}` },
  });

  // Summary
  const statusIcon = qaReport.ready ? 'PASSED' : 'ISSUES FOUND';
  blocks.push({
    type: 'section',
    text: {
      type: 'mrkdwn',
      text: `*Status:* ${statusIcon}\n` +
        `*Total campaigns:* ${qaReport.totalCampaigns}\n` +
        `*Passed:* ${qaReport.passed} | *Failed:* ${qaReport.failed}\n` +
        `*Total prospects:* ${totalProspects}`,
    },
  });

  // Tier breakdown
  for (const [tier, result] of Object.entries(dripResults)) {
    const resultList = Array.isArray(result) ? result : [result];
    const totalContacts = resultList.reduce((s, r) => s + (r.contactCount || 0), 0);
    const totalCampaigns = resultList.reduce((s, r) => s + (r.campaigns?.length || 0), 0);
    blocks.push({
      type: 'section',
      text: {
        type: 'mrkdwn',
        text: `*${tier}:* ${totalContacts} contacts, ${totalCampaigns} campaigns`,
      },
    });
  }

  // Issues (if any)
  if (qaReport.issues.length > 0) {
    blocks.push({ type: 'divider' });
    blocks.push({
      type: 'section',
      text: {
        type: 'mrkdwn',
        text: '*Issues:*\n' + qaReport.issues.slice(0, 10).map(i => `- ${i}`).join('\n') +
          (qaReport.issues.length > 10 ? `\n...and ${qaReport.issues.length - 10} more` : ''),
      },
    });
  }

  // Approval buttons
  blocks.push({ type: 'divider' });
  blocks.push({
    type: 'actions',
    elements: [
      {
        type: 'button',
        text: { type: 'plain_text', text: 'Approve & Send' },
        action_id: 'links:brevo:approve',
        style: 'primary',
        value: JSON.stringify({ clientName: clientData.Name }),
      },
      {
        type: 'button',
        text: { type: 'plain_text', text: 'Send Test Email' },
        action_id: 'links:brevo:test',
        value: JSON.stringify({ clientName: clientData.Name }),
      },
      {
        type: 'button',
        text: { type: 'plain_text', text: 'Cancel All' },
        action_id: 'links:brevo:cancel',
        style: 'danger',
        value: JSON.stringify({ clientName: clientData.Name }),
      },
    ],
  });

  return blocks;
}

/**
 * Log financial data for campaign costs and expected revenue.
 */
async function logCampaignFinancials(clientData, campaigns, dripResults) {
  const dateStr = new Date().toISOString().split('T')[0];
  const totalEmails = campaigns.length * 4; // 4 steps per prospect
  const campaignCost = totalEmails * BREVO_COST_PER_EMAIL;

  // Log campaign cost
  try {
    await db.logFinance({
      client: clientData.Name,
      type: 'Cost',
      category: 'Brevo',
      amount: Math.round(campaignCost * 100) / 100,
      description: `Drip campaign: ${campaigns.length} prospects x 4 emails = ${totalEmails} emails`,
      date: dateStr,
    });
  } catch (err) {
    console.error('[link-builder] Failed to log campaign cost:', err.message);
  }

  // Calculate and log expected revenue
  try {
    let expectedLinks = 0;
    for (const [tier, tierCampaigns] of Object.entries(brevo.segmentByTier(campaigns))) {
      const count = tierCampaigns.length;
      const replyRate = EXPECTED_REPLY_RATE[tier] || 0.05;
      const convRate = EXPECTED_CONVERSION_RATE[tier] || 0.15;
      expectedLinks += count * replyRate * convRate;
    }

    const expectedRevenue = Math.round(expectedLinks * LINK_PRICE);

    await db.logFinance({
      client: clientData.Name,
      type: 'Revenue',
      category: 'Link Fee',
      amount: expectedRevenue,
      description: `Projected: ${Math.round(expectedLinks * 10) / 10} links from ${campaigns.length} prospects (not yet realized)`,
      date: dateStr,
    });
  } catch (err) {
    console.error('[link-builder] Failed to log expected revenue:', err.message);
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
 * Handle button actions from Slack
 */
async function handleAction(action, data, context) {
  const { channel, message_ts, thread_ts } = context;
  const threadRef = thread_ts || message_ts;

  switch (action) {
    case 'push': {
      // Parse client name from button value
      let clientName;
      try {
        const parsed = JSON.parse(data.value || '{}');
        clientName = parsed.clientName;
      } catch { /* ignore */ }

      // Get cached campaign data
      const cached = clientName ? _lastCampaignCache.get(clientName) : null;
      if (!cached) {
        await slack.post(channel, 'Campaign data expired. Please re-run the pipeline first.', { thread_ts: threadRef });
        break;
      }

      await slack.post(channel, 'Creating tier-segmented drip sequences in Brevo...', { thread_ts: threadRef });

      try {
        const result = await pushToBrevo(cached.campaigns, cached.clientData, { channel, thread_ts: threadRef });

        const campaignCount = result.campaignIds.length;
        const qaStatus = result.qaReport?.ready ? 'PASSED' : 'NEEDS REVIEW';

        await slack.post(
          channel,
          `Brevo push complete:\n- ${campaignCount} campaigns created (4-step drip)\n- QA: ${qaStatus}\n- ${result.qaReport?.issues?.length || 0} issues found\n\nCheck the QA report above for details.`,
          { thread_ts: threadRef }
        );
      } catch (err) {
        await slack.post(channel, `Brevo push failed: ${err.message}`, { thread_ts: threadRef });
      }
      break;
    }

    case 'brevo:approve': {
      // Approve and send all scheduled campaigns
      let clientName;
      try {
        const parsed = JSON.parse(data.value || '{}');
        clientName = parsed.clientName;
      } catch { /* ignore */ }

      await slack.post(channel, 'Campaigns approved. Scheduled sends will fire on their designated days (Day 0, 3, 7, 14).', { thread_ts: threadRef });
      break;
    }

    case 'brevo:test': {
      // Send test email to Jeff
      await slack.post(channel, 'Test email feature: use Brevo dashboard to send test emails for now.', { thread_ts: threadRef });
      break;
    }

    case 'brevo:cancel': {
      // Cancel all drip campaigns
      let clientName;
      try {
        const parsed = JSON.parse(data.value || '{}');
        clientName = parsed.clientName;
      } catch { /* ignore */ }

      const cached = clientName ? _lastCampaignCache.get(clientName) : null;
      if (cached?._lastBrevoPush?.campaignIds) {
        const cancelled = await brevo.cancelDripForReply(cached._lastBrevoPush.campaignIds);
        const cancelledCount = cancelled.filter(r => r.status === 'cancelled').length;
        await slack.post(channel, `Cancelled ${cancelledCount} campaigns.`, { thread_ts: threadRef });
      } else {
        await slack.post(channel, 'No active campaigns found to cancel.', { thread_ts: threadRef });
      }
      break;
    }

    case 'edit': {
      await slack.post(channel, 'Edit mode coming soon. For now, regenerate with `/links [client] [count]`.', { thread_ts: threadRef });
      break;
    }

    case 'cancel': {
      await slack.post(channel, 'Campaign cancelled.', { thread_ts: threadRef });
      break;
    }

    default:
      console.log('Unknown link action:', action);
  }
}

module.exports = { execute, handleAction, pushToBrevo };
