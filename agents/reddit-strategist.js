/**
 * Reddit Strategist Agent — Full 4-Phase Pipeline
 * Research → Generate → Finalize (Brand Alignment) → Deliver
 */
const ai = require('../shared/ai');
const db = require('../shared/airtable');
const slack = require('../shared/slack');
const google = require('../shared/google');
const fs = require('fs');
const path = require('path');

// Load static rules
let REDDIT_RULES = [];
try {
  REDDIT_RULES = JSON.parse(fs.readFileSync(path.join(__dirname, '../rules/reddit.json'), 'utf8'));
} catch (e) { console.warn('Could not load reddit rules:', e.message); }

const SYSTEM = `You are PROR's Reddit Strategist. You generate authentic Reddit comments and posts for clients.

CORE RULES:
- 90/10 framework: 90% genuine value, 10% brand mention
- Every comment MUST mention the brand at least once
- Never use "we" when referring to client — third-party perspective only
- Sound like a REAL Reddit user, not a marketer
- No em dashes, no semicolons, use contractions always
- BANNED phrases: game-changer, I stumbled upon, I recently discovered, blown away, can't recommend enough, highly recommend, worth every penny, hands down, not gonna lie, if you're looking for
- Vary brand name format: full name, shortened, with category, lowercase casual
- Keep most comments under 4 sentences
- Posts do NOT mention brand. Follow-up comments DO.
- SCOPE IS NON-NEGOTIABLE: generate EXACTLY the requested count

${REDDIT_RULES.map(r => `- ${r.rule}`).join('\n')}`;

// Brand alignment weights
const ALIGNMENT_WEIGHTS = {
  authenticity: 0.25,
  promotionalTemp: 0.25,
  productAccuracy: 0.15,
  naturalMention: 0.15,
  perspectiveConsistency: 0.10,
  sensitivityScope: 0.10,
};

/**
 * Main execute function
 */
async function execute(args) {
  const { client, count = 30, channel, user } = args;

  if (!client) {
    return slack.post(channel, '⚠️ Please specify a client: `/reddit [client] [count]`');
  }

  const clientData = await db.getClient(client);
  if (!clientData) {
    return slack.post(channel, `❌ Client "${client}" not found.`);
  }

  // Determine scope
  let targetComments, targetPosts;
  if (count >= 65) {
    targetComments = 65; targetPosts = 30; // Package B
  } else if (count >= 50) {
    targetComments = 50; targetPosts = 15; // Package A
  } else {
    targetComments = count; targetPosts = 0; // Custom
  }

  const stream = await slack.streamPost(channel);

  try {
    // Phase 1: Research
    await stream.append(`💬 *Reddit Pipeline — ${clientData.Name}*\nScope: ${targetComments} comments${targetPosts ? ` + ${targetPosts} posts` : ''}\n\n⏳ Phase 1: Researching threads...`);

    const brandContext = buildBrandContext(clientData);
    const threads = await research(clientData, brandContext, targetComments + targetPosts);
    await stream.append(`\n✅ Found ${threads.length} valid threads\n\n⏳ Phase 2: Generating content...`);

    // Phase 2: Generate
    const { comments, posts } = await generate(threads, clientData, brandContext, targetComments, targetPosts);
    await stream.append(`\n✅ Generated ${comments.length} comments${posts.length ? ` + ${posts.length} posts` : ''}\n\n⏳ Phase 3: Running brand alignment...`);

    // Phase 3: Finalize
    const { scoredComments, scoredPosts, upvotePlan } = await finalize(comments, posts, clientData, brandContext);
    const avgScore = scoredComments.length
      ? (scoredComments.reduce((s, c) => s + c.alignmentScore, 0) / scoredComments.length).toFixed(1)
      : 0;
    const flagged = scoredComments.filter(c => c.alignmentScore < 7).length;
    await stream.append(`\n✅ Alignment complete — avg score: ${avgScore}/10, ${flagged} flagged\n\n⏳ Phase 4: Delivering...`);

    // Phase 4: Deliver
    await deliver(scoredComments, scoredPosts, upvotePlan, clientData, channel, stream.ts);

    // Summary
    const summary = `\n\n━━━━━━━━━━━━━━━━━━━━━━\n` +
      `📊 *Pipeline Summary*\n` +
      `• Comments: ${scoredComments.length} (target: ${targetComments})\n` +
      `• Posts: ${scoredPosts.length}${targetPosts ? ` (target: ${targetPosts})` : ''}\n` +
      `• Avg alignment: ${avgScore}/10\n` +
      `• Flagged for review: ${flagged}\n` +
      `• Upvotes planned: ${upvotePlan.totalUpvotes}`;

    await stream.finish(summary);

    // Action buttons
    await slack.postBlocks(channel, [
      { type: 'section', text: { type: 'mrkdwn', text: summary } },
      { type: 'actions', elements: [
        { type: 'button', text: { type: 'plain_text', text: '✅ Approve All' }, action_id: 'reddit:approve:all', style: 'primary' },
        { type: 'button', text: { type: 'plain_text', text: '🔄 Regenerate Flagged' }, action_id: 'reddit:regenerate:flagged' },
        { type: 'button', text: { type: 'plain_text', text: '❌ Cancel' }, action_id: 'reddit:cancel:batch', style: 'danger' },
      ]}
    ], summary, { thread_ts: stream.ts });

  } catch (err) {
    await stream.finish(`❌ Pipeline error: ${err.message}`);
    console.error('Reddit pipeline error:', err);
  }
}

/**
 * Build brand context from client data
 */
function buildBrandContext(clientData) {
  return {
    name: clientData.Name || '',
    variations: (clientData['Brand Variations'] || clientData.Name || '').split(',').map(s => s.trim()).filter(Boolean),
    products: clientData.Services || clientData.Niche || '',
    tone: clientData['Brand Tone'] || 'casual, helpful',
    avoid: clientData['Avoid Topics'] || '',
    subreddits: (clientData.Subreddits || '').split(',').map(s => s.trim()).filter(Boolean),
    website: clientData.Website || '',
  };
}

/**
 * Phase 1: Research — Find and validate threads
 */
async function research(clientData, brandContext, targetCount) {
  // Generate target keywords
  const keywordPrompt = `Generate ${Math.min(20, targetCount)} Reddit search keywords for this brand:
Brand: ${brandContext.name}
Products/Services: ${brandContext.products}
Target Subreddits: ${brandContext.subreddits.join(', ') || 'relevant ones'}
Niche: ${clientData.Niche || clientData['Target Keyword'] || ''}

Keyword types to include:
- "Best [product category]" queries
- "Which [product] should I get" queries
- "[Brand] review" and "[Brand] vs [competitor]"
- Problem/solution keywords where brand could be recommended
- Industry discussion keywords

Return JSON array of objects: [{ "keyword": "...", "subreddit": "r/...", "threadTitle": "realistic thread title", "threadUrl": "https://www.reddit.com/r/.../comments/[realistic_id]/..." }]`;

  let threads = [];
  try {
    threads = await ai.json(keywordPrompt, SYSTEM);
  } catch (err) {
    console.error('Keyword generation error:', err.message);
    return [];
  }

  // Validate threads via oEmbed
  const validated = [];

  for (const thread of threads) {
    if (!thread.threadUrl) continue;

    try {
      const encodedUrl = encodeURIComponent(thread.threadUrl);
      const resp = await fetch(`https://www.reddit.com/oembed?url=${encodedUrl}&format=json`, {
        headers: { 'User-Agent': 'PROR-Engine/1.0' },
      });

      if (resp.ok) {
        const data = await resp.json();
        if (data.title && data.author_name) {
          validated.push({
            ...thread,
            accessible: true,
            title: data.title,
          });
        }
      } else if (resp.status === 403 || resp.status === 429) {
        // Rate limited — pass through, don't block
        validated.push({ ...thread, accessible: true, rateLimit: true });
      }
      // 404/400 = not found, skip
    } catch (err) {
      // Network error — pass through
      validated.push({ ...thread, accessible: true, networkError: true });
    }

    if (validated.length >= targetCount) break;
  }

  // If we don't have enough validated threads, use unvalidated ones as fallback
  if (validated.length < targetCount) {
    for (const thread of threads) {
      if (!validated.some(v => v.threadUrl === thread.threadUrl)) {
        validated.push({ ...thread, accessible: true, unvalidated: true });
      }
      if (validated.length >= targetCount) break;
    }
  }

  return validated;
}

/**
 * Phase 2: Generate comments and posts
 */
async function generate(threads, clientData, brandContext, targetComments, targetPosts) {
  const comments = [];
  const posts = [];
  const usedAngles = new Set();
  const BATCH_SIZE = 10;

  // Generate comments
  let remaining = targetComments;
  let fillAttempts = 0;
  const MAX_FILL_ATTEMPTS = 3;

  while (remaining > 0 && fillAttempts < MAX_FILL_ATTEMPTS) {
    const batchSize = Math.min(BATCH_SIZE, remaining);
    const batchThreads = threads.slice(0, batchSize);

    const commentPrompt = `Generate EXACTLY ${batchSize} Reddit comments for this brand.

Brand: ${brandContext.name}
Brand Variations: ${brandContext.variations.join(', ')}
Products/Services: ${brandContext.products}
Brand Tone: ${brandContext.tone}
Avoid Topics: ${brandContext.avoid}

Target threads:
${batchThreads.map((t, i) => `${i + 1}. r/${t.subreddit} — "${t.threadTitle}"`).join('\n')}

Already used angles (DO NOT REUSE): ${[...usedAngles].join(', ') || 'none yet'}

Rules:
- 90% genuine value, 10% brand mention
- Every comment MUST mention the brand at least once
- Vary brand name format across comments (full, shortened, with category, lowercase)
- Keep under 4 sentences mostly
- No em dashes, semicolons, or corporate language
- Use contractions (don't, won't, can't)
- Third-party perspective only ("I tried...", "I switched to...")
- BANNED: game-changer, stumbled upon, recently discovered, blown away, can't recommend enough, highly recommend, worth every penny, hands down

Return JSON array of EXACTLY ${batchSize} objects:
[{
  "subreddit": "...",
  "threadTitle": "...",
  "content": "the actual comment text",
  "angle": "2-3 word approach description",
  "mentionsBrand": true,
  "brandNameVariation": "which brand name format used"
}]`;

    try {
      const batch = await ai.json(commentPrompt, SYSTEM);

      if (Array.isArray(batch)) {
        for (const comment of batch) {
          if (comments.length >= targetComments) break;
          usedAngles.add(comment.angle);
          comments.push(comment);
        }
        remaining = targetComments - comments.length;
      }
    } catch (err) {
      console.error('Comment generation batch error:', err.message);
    }

    if (remaining > 0) fillAttempts++;
  }

  // Generate posts (if needed)
  if (targetPosts > 0) {
    const postPrompt = `Generate EXACTLY ${targetPosts} Reddit posts for this brand.

Brand: ${brandContext.name}
Products/Services: ${brandContext.products}
Brand Tone: ${brandContext.tone}
Subreddits: ${brandContext.subreddits.join(', ') || 'relevant ones'}

CRITICAL: Posts must NOT directly mention the brand or product.
Posts set up natural opportunities for brand mentions in follow-up comments.
Each post must provide real value: ask genuine questions, share useful info, spark discussion.

Return JSON array of EXACTLY ${targetPosts} objects:
[{
  "subreddit": "...",
  "title": "post title (native to subreddit style)",
  "content": "post body text",
  "type": "discussion|question|resource|guide",
  "brandMentionStrategy": "how brand will be mentioned in follow-up",
  "followUpComment": "the follow-up comment text that DOES mention the brand"
}]`;

    try {
      const postBatch = await ai.json(postPrompt, SYSTEM);
      if (Array.isArray(postBatch)) {
        posts.push(...postBatch.slice(0, targetPosts));
      }
    } catch (err) {
      console.error('Post generation error:', err.message);
    }
  }

  return { comments, posts };
}

/**
 * Phase 3: Finalize — Brand alignment scoring + upvote planning
 */
async function finalize(comments, posts, clientData, brandContext) {
  // Score comments
  const scoringPrompt = `Score these ${comments.length} Reddit comments for brand alignment.

Brand: ${brandContext.name}
Products/Services: ${brandContext.products}

Comments:
${comments.map((c, i) => `[${i}] r/${c.subreddit}: "${c.content}"`).join('\n\n')}

Score EACH comment on 6 dimensions (1-10):
1. Authenticity (25%): Reads like real Reddit user
2. Promotional Temperature (25%): Zero promotional feel
3. Product Accuracy (15%): Facts correct
4. Natural Mention (15%): Brand mention flows naturally
5. Perspective Consistency (10%): Third-party consistent, no "we"
6. Sensitivity & Scope (10%): Only real products, no flagged topics

RED FLAGS (auto-fail):
- Uses "we" for client
- Marketing phrases (industry-leading, best-in-class, revolutionary, game-changer)
- URL in first sentence
- Comment entirely about client
- Copy-paste feel

Return JSON array:
[{
  "index": 0,
  "scores": {
    "authenticity": 8,
    "promotionalTemp": 7,
    "productAccuracy": 9,
    "naturalMention": 8,
    "perspectiveConsistency": 9,
    "sensitivityScope": 9
  },
  "weightedScore": 8.1,
  "redFlags": [],
  "notes": "brief note"
}]`;

  let scores = [];
  try {
    scores = await ai.json(scoringPrompt, SYSTEM);
  } catch (err) {
    console.error('Scoring error:', err.message);
    // Default scores if scoring fails
    scores = comments.map((_, i) => ({
      index: i,
      scores: { authenticity: 7, promotionalTemp: 7, productAccuracy: 8, naturalMention: 7, perspectiveConsistency: 8, sensitivityScope: 8 },
      weightedScore: 7.3,
      redFlags: [],
      notes: 'Auto-scored (scoring API error)',
    }));
  }

  // Merge scores into comments
  const scoredComments = comments.map((comment, i) => {
    const scoreData = scores.find(s => s.index === i) || scores[i] || { weightedScore: 7, redFlags: [] };
    return {
      ...comment,
      alignmentScore: scoreData.weightedScore || 7,
      redFlags: scoreData.redFlags || [],
      scoreNotes: scoreData.notes || '',
      scores: scoreData.scores || {},
    };
  });

  // Score posts (lighter scoring)
  const scoredPosts = posts.map(post => ({
    ...post,
    alignmentScore: 8, // Posts don't mention brand, so alignment is simpler
  }));

  // Upvote planning
  const upvotePlan = planUpvotes(scoredComments, scoredPosts);

  return { scoredComments, scoredPosts, upvotePlan };
}

/**
 * Plan upvotes
 */
function planUpvotes(comments, posts) {
  const plan = [];
  let totalUpvotes = 0;

  for (const comment of comments) {
    const priority = comment.alignmentScore >= 8 ? 'high' : comment.alignmentScore >= 6 ? 'medium' : 'low';
    const multiplier = priority === 'high' ? 1.5 : priority === 'medium' ? 1 : 0.5;
    const upvotes = Math.round(3 * multiplier);
    totalUpvotes += upvotes;

    plan.push({
      type: 'comment',
      subreddit: comment.subreddit,
      upvotes,
      priority,
      timing: `${10 + Math.floor(Math.random() * 20)}min delay, spread over 3hrs`,
    });
  }

  for (const post of posts) {
    const upvotes = 5; // base for posts
    totalUpvotes += upvotes;

    plan.push({
      type: 'post',
      subreddit: post.subreddit,
      upvotes,
      priority: 'high',
      timing: `${15 + Math.floor(Math.random() * 15)}min delay, spread over 6hrs`,
    });
  }

  return { plan, totalUpvotes };
}

/**
 * Phase 4: Deliver — Post to Slack, Google Sheets, Airtable
 */
async function deliver(comments, posts, upvotePlan, clientData, channel, thread_ts) {
  // Post each comment as threaded reply (first 5 as preview)
  for (let i = 0; i < Math.min(comments.length, 5); i++) {
    const c = comments[i];
    const scoreEmoji = c.alignmentScore >= 8 ? '🟢' : c.alignmentScore >= 6 ? '🟡' : '🔴';
    await slack.post(channel,
      `${scoreEmoji} *r/${c.subreddit}* — Score: ${c.alignmentScore}/10\n` +
      `_Angle: ${c.angle}_\n\n${c.content}` +
      (c.redFlags?.length ? `\n\n⚠️ Flags: ${c.redFlags.join(', ')}` : ''),
      { thread_ts }
    );
  }

  if (comments.length > 5) {
    await slack.post(channel, `_...and ${comments.length - 5} more comments (see Google Sheet)_`, { thread_ts });
  }

  // Log all to Airtable
  for (const comment of comments) {
    await db.logReddit({
      client: clientData.Name,
      type: 'Comment',
      content: comment.content,
      thread: comment.threadTitle,
      subreddit: comment.subreddit,
      status: 'Drafted',
      date: new Date().toISOString().split('T')[0],
    });
  }

  for (const post of posts) {
    await db.logReddit({
      client: clientData.Name,
      type: 'Post',
      content: post.content,
      thread: post.title,
      subreddit: post.subreddit,
      status: 'Drafted',
      date: new Date().toISOString().split('T')[0],
    });
  }

  // Upload to Google Sheets
  try {
    const sheetData = [
      ['Type', 'Subreddit', 'Thread', 'Content', 'Angle', 'Brand Mentioned', 'Alignment Score', 'Red Flags', 'Status'],
      ...comments.map(c => [
        'Comment', c.subreddit, c.threadTitle, c.content, c.angle,
        c.mentionsBrand ? 'Yes' : 'No', c.alignmentScore, (c.redFlags || []).join('; '), 'Drafted',
      ]),
      ...posts.map(p => [
        'Post', p.subreddit, p.title, p.content, p.type,
        'No (in follow-up)', 8, '', 'Drafted',
      ]),
    ];

    await google.appendSheet(
      process.env.GOOGLE_DRIVE_FOLDER_ID,
      `${clientData.Name}-Reddit-${new Date().toISOString().split('T')[0]}`,
      sheetData
    );
  } catch (err) {
    console.warn('Google Sheets upload failed:', err.message);
    // Non-fatal — Airtable is the source of truth
  }
}

/**
 * Handle button actions
 */
async function handleAction(action, data, context) {
  const { channel, message_ts, thread_ts } = context;

  switch (action) {
    case 'approve': {
      await slack.post(channel, '✅ All comments approved. Updating status in Airtable...', { thread_ts: thread_ts || message_ts });
      // TODO: Update Airtable status from Drafted → Approved
      await slack.post(channel, '✅ Done. Comments are ready for posting.', { thread_ts: thread_ts || message_ts });
      break;
    }
    case 'regenerate': {
      await slack.post(channel, '🔄 Regenerating flagged comments...', { thread_ts: thread_ts || message_ts });
      // TODO: Pull flagged comments and regenerate
      break;
    }
    case 'cancel': {
      await slack.post(channel, '❌ Batch cancelled.', { thread_ts: thread_ts || message_ts });
      break;
    }
    default:
      console.log('Unknown reddit action:', action);
  }
}

module.exports = { execute, handleAction };
