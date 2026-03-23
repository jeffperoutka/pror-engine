/**
 * Shared Slack utilities for PROR Engine
 * All agents use these to interact with Slack
 */
const { WebClient } = require('@slack/web-api');

let _client;
function getClient() {
  if (!_client) _client = new WebClient(process.env.SLACK_BOT_TOKEN);
  return _client;
}

// Channel map — all agents post to the right channel
const CHANNELS = {
  command: () => process.env.CHANNEL_COMMAND_CENTER,
  links: () => process.env.CHANNEL_LINK_BUILDING,
  reddit: () => process.env.CHANNEL_REDDIT,
  clients: () => process.env.CHANNEL_CLIENTS,
  qa: () => process.env.CHANNEL_QA,
  finances: () => process.env.CHANNEL_FINANCES,
};

/**
 * Post a message to a channel or thread
 */
async function post(channelOrId, text, opts = {}) {
  const slack = getClient();
  return slack.chat.postMessage({
    channel: channelOrId,
    text,
    ...opts,
  });
}

/**
 * Post blocks (rich formatting) to a channel or thread
 */
async function postBlocks(channelOrId, blocks, text = '', opts = {}) {
  const slack = getClient();
  return slack.chat.postMessage({
    channel: channelOrId,
    blocks,
    text, // fallback
    ...opts,
  });
}

/**
 * Update an existing message
 */
async function update(channel, ts, text, opts = {}) {
  const slack = getClient();
  return slack.chat.update({
    channel,
    ts,
    text,
    ...opts,
  });
}

/**
 * Stream-style updates: post initial message then update it
 */
async function streamPost(channel, thread_ts = null) {
  const msg = await post(channel, '⏳ Thinking...', { thread_ts });
  let buffer = '';

  return {
    ts: msg.ts,
    channel: msg.channel,
    append: async (chunk) => {
      buffer += chunk;
      await update(msg.channel, msg.ts, buffer, { thread_ts });
    },
    finish: async (finalText) => {
      await update(msg.channel, msg.ts, finalText || buffer, { thread_ts });
    },
  };
}

/**
 * Upload a file to a channel/thread
 */
async function uploadFile(channel, filename, content, opts = {}) {
  const slack = getClient();
  return slack.filesUploadV2({
    channel_id: channel,
    filename,
    content: typeof content === 'string' ? Buffer.from(content) : content,
    ...opts,
  });
}

/**
 * Add a reaction to a message
 */
async function react(channel, ts, emoji) {
  const slack = getClient();
  try {
    await slack.reactions.add({ channel, timestamp: ts, name: emoji });
  } catch (e) {
    // Non-fatal — don't crash the pipeline over a missing reaction
    console.warn(`React failed (${emoji}):`, e.data?.error || e.message);
  }
}

/**
 * Get thread messages
 */
async function getThread(channel, ts) {
  const slack = getClient();
  const result = await slack.conversations.replies({ channel, ts });
  return result.messages || [];
}

/**
 * Verify Slack request signature
 */
function verifySignature(signingSecret, signature, timestamp, body) {
  const crypto = require('crypto');
  const sigBasestring = `v0:${timestamp}:${body}`;
  const mySignature = 'v0=' + crypto.createHmac('sha256', signingSecret)
    .update(sigBasestring, 'utf8')
    .digest('hex');
  return crypto.timingSafeEqual(
    Buffer.from(mySignature, 'utf8'),
    Buffer.from(signature, 'utf8')
  );
}

/**
 * Parse incoming Slack request and verify
 */
function parseAndVerify(req) {
  const signature = req.headers['x-slack-signature'];
  const timestamp = req.headers['x-slack-request-timestamp'];
  const rawBody = typeof req.body === 'string' ? req.body : JSON.stringify(req.body);

  // Check timestamp isn't too old (5 min)
  const time = Math.floor(new Date().getTime() / 1000);
  if (Math.abs(time - timestamp) > 300) return false;

  return verifySignature(process.env.SLACK_SIGNING_SECRET, signature, timestamp, rawBody);
}

module.exports = {
  getClient,
  CHANNELS,
  post,
  postBlocks,
  update,
  streamPost,
  uploadFile,
  react,
  getThread,
  verifySignature,
  parseAndVerify,
};
