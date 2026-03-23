/**
 * Engain Webhook Handler — PROR Engine
 *
 * Receives task.completed events from Engain after Reddit actions are executed.
 *
 * Flow:
 * 1. Verify webhook signature (if ENGAIN_WEBHOOK_SECRET is set)
 * 2. Update Airtable record: status "Submitted" -> "Live", store Reddit URL
 * 3. Post confirmation to Slack #reddit channel
 * 4. For comments: auto-schedule follow-up reply + staggered upvotes (2-8 hours later)
 *
 * Configure in Engain dashboard: webhook URL = https://pror-engine.vercel.app/api/webhooks/engain
 */
const { waitUntil } = require('@vercel/functions');
const crypto = require('crypto');
const engain = require('../../shared/engain');
const slack = require('../../shared/slack');
const db = require('../../shared/airtable');

module.exports = async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  // ── Verify webhook signature ──
  const secret = process.env.ENGAIN_WEBHOOK_SECRET;
  if (secret) {
    const signature = req.headers['x-engain-signature'] || req.headers['x-webhook-signature'] || '';
    const rawBody = typeof req.body === 'string' ? req.body : JSON.stringify(req.body);
    const expected = crypto.createHmac('sha256', secret).update(rawBody).digest('hex');
    if (!crypto.timingSafeEqual(Buffer.from(signature), Buffer.from(expected))) {
      console.error('[Engain Webhook] Signature verification failed');
      return res.status(401).json({ error: 'Invalid signature' });
    }
  }

  const payload = req.body;
  const event = payload?.event;

  console.log(`[Engain Webhook] Event: ${event}, task_id: ${payload?.task_id}`);

  // Acknowledge immediately — process in background
  res.status(200).json({ ok: true });

  if (event === 'task.completed') {
    waitUntil(
      handleTaskCompleted(payload).catch(err => {
        console.error('[Engain Webhook] task.completed handler failed:', err.message, err.stack);
      })
    );
  }
};

/**
 * Handle task.completed event
 *
 * Payload shape: { task_id, type, status, result: { url } }
 */
async function handleTaskCompleted(payload) {
  const { task_id, type, status, result } = payload;
  const redditUrl = result?.url;

  console.log(`[Engain Webhook] Task ${task_id} completed: ${type} -> ${status}, URL: ${redditUrl || 'none'}`);

  if (status !== 'completed' && status !== 'published') {
    console.log(`[Engain Webhook] Task ${task_id} status "${status}" — not a success. Skipping.`);
    return;
  }

  // ── Find and update Airtable record ──
  const base = db.getBase();
  let record = null;

  try {
    // Find the Reddit record with this Engain task ID
    const records = await base('Reddit').select({
      filterByFormula: `{Engain Task ID} = "${task_id}"`,
      maxRecords: 1,
    }).all();

    if (records.length > 0) {
      record = records[0];

      // Update status from "Submitted" to "Live" and store the Reddit URL
      const updateFields = {
        Status: 'Live',
        'Published Date': new Date().toISOString().split('T')[0],
      };
      if (redditUrl) {
        updateFields['Reddit URL'] = redditUrl;
      }

      await base('Reddit').update(record.id, updateFields);
      console.log(`[Engain Webhook] Updated Airtable record ${record.id}: Live, URL: ${redditUrl}`);
    } else {
      console.log(`[Engain Webhook] No Airtable record found for task ${task_id}`);
    }
  } catch (err) {
    console.error(`[Engain Webhook] Airtable update failed for task ${task_id}:`, err.message);
  }

  // ── Post confirmation to Slack #reddit channel ──
  const redditChannel = slack.CHANNELS.reddit();
  if (redditChannel) {
    try {
      const typeLabel = type === 'post' ? 'Post' : type === 'comment' ? 'Comment' : type === 'reply' ? 'Reply' : type;
      const client = record?.fields?.Client || 'Unknown';
      const subreddit = record?.fields?.Subreddit || '';

      const lines = [
        `*${typeLabel} Published* — ${client}`,
        subreddit ? `r/${subreddit}` : '',
        redditUrl ? `<${redditUrl}|View on Reddit>` : '',
      ].filter(Boolean);

      await slack.post(redditChannel, lines.join('\n'));
    } catch (err) {
      console.error('[Engain Webhook] Slack notification failed:', err.message);
    }
  }

  // ── Auto-schedule follow-ups for comments ──
  if (type === 'comment' && redditUrl && record) {
    await scheduleFollowUps(record, redditUrl);
  }

  // ── Auto-schedule upvotes for all content types ──
  if (redditUrl) {
    await scheduleUpvotes(redditUrl, type);
  }
}

/**
 * Schedule a follow-up reply to a comment, staggered 2-8 hours later.
 * Only if the Airtable record has a "Follow-Up Text" field.
 */
async function scheduleFollowUps(record, redditUrl) {
  const followUpText = record.fields?.['Follow-Up Text'];
  if (!followUpText) return;

  try {
    // Stagger 2-8 hours after the original comment
    const delayHours = 2 + Math.random() * 6;
    const delayMs = delayHours * 60 * 60 * 1000;
    const scheduleAt = new Date(Date.now() + delayMs).toISOString();

    const result = await engain.createReply(redditUrl, followUpText, scheduleAt);
    console.log(`[Engain Webhook] Follow-up reply scheduled: ${result.id || result.task_id} in ${delayHours.toFixed(1)}h`);

    // Store the follow-up task ID back in Airtable
    const base = db.getBase();
    await base('Reddit').update(record.id, {
      'Follow-Up Task ID': result.id || result.task_id || '',
    });
  } catch (err) {
    console.error(`[Engain Webhook] Follow-up scheduling failed:`, err.message);
  }
}

/**
 * Schedule upvotes on published content, staggered 2-8 hours later.
 * Comments get 3-5 upvotes, posts get 5-8.
 */
async function scheduleUpvotes(redditUrl, type) {
  try {
    const count = type === 'post' ? 5 + Math.floor(Math.random() * 4) : 3 + Math.floor(Math.random() * 3);
    const delayHours = 2 + Math.random() * 6;
    const delayMs = delayHours * 60 * 60 * 1000;
    const scheduleAt = new Date(Date.now() + delayMs).toISOString();

    const result = await engain.createUpvote(redditUrl, count, scheduleAt);
    console.log(`[Engain Webhook] ${count} upvotes scheduled for ${redditUrl} in ${delayHours.toFixed(1)}h (task: ${result.id || result.task_id})`);
  } catch (err) {
    console.error(`[Engain Webhook] Upvote scheduling failed:`, err.message);
  }
}
