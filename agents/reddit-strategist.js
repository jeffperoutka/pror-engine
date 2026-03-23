/**
 * Reddit Strategist Agent
 * Generates Reddit comments/posts for clients with brand alignment
 * Ported from reddit-strategy bot
 */
const ai = require('../shared/ai');
const db = require('../shared/airtable');
const slack = require('../shared/slack');
const fs = require('fs');
const path = require('path');

// Load rules
let rules = [];
try {
  rules = JSON.parse(fs.readFileSync(path.join(__dirname, '../rules/reddit.json'), 'utf8'));
} catch { /* rules file may not exist yet */ }

const SYSTEM = `You are an expert Reddit strategist. You generate authentic Reddit comments and posts that subtly promote brands while providing genuine value to the community.

RULES:
${rules.map(r => `- ${r.rule}`).join('\n')}

CRITICAL RULES:
- Every comment must be unique — different angles, different personas, different brand name variations
- When a specific count is requested, generate EXACTLY that count. Not fewer.
- Never be overtly promotional. Sound like a real Reddit user sharing genuine experience.
- Match the tone/language of the subreddit (casual in r/gadgets, technical in r/SEO, etc.)
- Include specific details that make comments believable (timeframes, specific results, comparisons)
- Vary comment length: mix short (1-2 sentences) and long (paragraph) responses
- Some comments should NOT mention the brand at all — just be helpful to build karma`;

// ========== MAIN EXECUTE ==========

async function execute(args) {
  const { client, count, channel } = args;

  if (!client) {
    return slack.post(channel, '⚠️ Usage: `/reddit [client] [count]`');
  }

  const clientData = await db.getClient(client);
  if (!clientData) {
    return slack.post(channel, `❌ Client "${client}" not found.`);
  }

  const targetCount = count || 30;

  const stream = await slack.streamPost(channel);

  try {
    await stream.append(`🎯 *Generating ${targetCount} Reddit comments for ${clientData.Name}*\n\n`);

    // Build brand context
    const brandContext = buildBrandContext(clientData);

    // Generate comments in batches
    const allComments = [];
    const batchSize = 10;
    const batches = Math.ceil(targetCount / batchSize);

    for (let i = 0; i < batches; i++) {
      const remaining = targetCount - allComments.length;
      const batchCount = Math.min(batchSize, remaining);

      await stream.append(`📝 Generating batch ${i + 1}/${batches} (${batchCount} comments)...\n`);

      const comments = await generateComments(clientData, brandContext, batchCount, allComments);
      allComments.push(...comments);
    }

    await stream.finish(
      `✅ *${allComments.length} Reddit comments ready for ${clientData.Name}*\n\n` +
      `Breakdown:\n` +
      `• Brand mentions: ${allComments.filter(c => c.mentionsBrand).length}\n` +
      `• Karma builders: ${allComments.filter(c => !c.mentionsBrand).length}\n` +
      `• Avg length: ${Math.round(allComments.reduce((s, c) => s + c.content.length, 0) / allComments.length)} chars\n\n` +
      `Review the full batch below. Click Approve to finalize.`
    );

    // Post each comment as a threaded reply for review
    for (let i = 0; i < allComments.length; i++) {
      const c = allComments[i];
      const prefix = c.mentionsBrand ? '🔵' : '⚪';
      await slack.post(channel,
        `${prefix} *#${i + 1}* — r/${c.subreddit}\n> ${c.threadTitle}\n\n${c.content}`,
        { thread_ts: stream.ts }
      );
    }

    // Post approve/reject buttons
    await slack.postBlocks(channel, [
      {
        type: 'actions',
        elements: [
          {
            type: 'button',
            text: { type: 'plain_text', text: '✅ Approve All' },
            action_id: `reddit:approve:${clientData.Name}`,
            style: 'primary',
          },
          {
            type: 'button',
            text: { type: 'plain_text', text: '🔄 Regenerate' },
            action_id: `reddit:regen:${clientData.Name}`,
          },
          {
            type: 'button',
            text: { type: 'plain_text', text: '❌ Cancel' },
            action_id: `reddit:cancel:${clientData.Name}`,
            style: 'danger',
          },
        ],
      },
    ], '', { thread_ts: stream.ts });

    // Log to Airtable
    for (const c of allComments) {
      await db.logReddit({
        client: clientData.Name,
        type: 'Comment',
        content: c.content,
        thread: c.threadTitle,
        subreddit: c.subreddit,
        status: 'Drafted',
      });
    }

  } catch (err) {
    await stream.finish(`❌ Error: ${err.message}`);
  }
}

// ========== GENERATION ==========

function buildBrandContext(client) {
  return {
    name: client.Name,
    variations: client['Brand Variations'] || client.Name,
    website: client.Website || '',
    niche: client.Niche || '',
    tone: client['Brand Tone'] || 'friendly, knowledgeable',
    targetSubreddits: (client.Subreddits || '').split(',').map(s => s.trim()).filter(Boolean),
    avoidTopics: client['Avoid Topics'] || '',
  };
}

async function generateComments(client, brand, count, existingComments = []) {
  const existingSummary = existingComments.length > 0
    ? `\n\nAlready generated ${existingComments.length} comments. The new batch must use COMPLETELY DIFFERENT angles and approaches. Here are the angles already used:\n${existingComments.map(c => `- ${c.angle}`).join('\n')}`
    : '';

  const prompt = `Generate exactly ${count} Reddit comments for ${brand.name}.

Brand Info:
- Name: ${brand.name}
- Variations: ${brand.variations}
- Website: ${brand.website}
- Niche: ${brand.niche}
- Tone: ${brand.tone}
- Target subreddits: ${brand.targetSubreddits.join(', ') || 'general niche subreddits'}
${existingSummary}

For each comment, return JSON array with objects containing:
{
  "subreddit": "subreddit name without r/",
  "threadTitle": "realistic thread title this would be a reply to",
  "content": "the actual comment text",
  "angle": "2-3 word description of the approach used",
  "mentionsBrand": true/false
}

Mix of brand mentions (60%) and pure karma builders (40%).
Vary lengths: some 1-2 sentences, some full paragraphs.
Every comment must feel like a real person typed it.`;

  const result = await ai.json(prompt, SYSTEM, { maxTokens: 8192 });
  return Array.isArray(result) ? result.slice(0, count) : [];
}

// ========== ACTION HANDLERS ==========

async function handleAction(action, data, context) {
  switch (action) {
    case 'approve':
      await slack.post(context.channel, `✅ Batch approved for ${data}. Comments marked as ready to post.`, {
        thread_ts: context.thread_ts,
      });
      // TODO: Update Airtable status to "Approved"
      // TODO: Optionally post through Engain API
      break;
    case 'regen':
      await slack.post(context.channel, `🔄 Regenerating batch for ${data}...`, {
        thread_ts: context.thread_ts,
      });
      // TODO: Trigger regeneration
      break;
    case 'cancel':
      await slack.post(context.channel, `🚫 Batch cancelled for ${data}.`, {
        thread_ts: context.thread_ts,
      });
      break;
  }
}

async function handleModal(action, values, context) {
  // Future: handle subreddit selection, brand config modals
}

module.exports = { execute, handleAction, handleModal };
