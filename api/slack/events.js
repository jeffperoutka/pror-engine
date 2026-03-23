/**
 * Slack Events API handler
 * Handles: app_mention, message (DMs and subscribed channels)
 * Routes everything through the Brain
 */
const { waitUntil } = require('@vercel/functions');
const slack = require('../../shared/slack');
const brain = require('../../shared/brain');

async function processEvent(body) {
  try {
    const event = body?.event;
    if (!event) return;

    // Ignore bot messages (prevent loops)
    if (event.bot_id || event.subtype === 'bot_message') return;

    const text = event.text || '';
    const channel = event.channel;
    const thread_ts = event.thread_ts || event.ts;
    const user = event.user;

    // Strip bot mention from text
    const cleanText = text.replace(/<@[A-Z0-9]+>/g, '').trim();
    if (!cleanText) return;

    // React to show we're processing
    await slack.react(channel, event.ts, 'eyes');

    // Route through the brain
    const { response, routing } = await brain.handleMessage(cleanText, {
      channel,
      thread: !!event.thread_ts,
      user,
    });

    // Post response in thread
    await slack.post(channel, response, { thread_ts });

    // Mark as done
    await slack.react(channel, event.ts, 'white_check_mark');

  } catch (err) {
    console.error('Event handler error:', err);
    try {
      const event = body?.event;
      if (event) {
        await slack.post(event.channel, `❌ Error: ${err.message}`, {
          thread_ts: event.thread_ts || event.ts,
        });
      }
    } catch (e) {
      console.error('Failed to post error:', e);
    }
  }
}

module.exports = async (req, res) => {
  // Handle URL verification challenge
  if (req.body?.type === 'url_verification') {
    return res.json({ challenge: req.body.challenge });
  }

  // Use waitUntil to keep the function alive after responding
  waitUntil(processEvent(req.body));

  // Acknowledge immediately (Slack needs response in 3s)
  res.status(200).json({ ok: true });
};
