/**
 * Shared Gmail module — OAuth2 client for PROR Engine
 * Handles token refresh, message listing, reading, replying, and marking as read.
 * Used by gmail-poll cron and daily-digest agent.
 */

const CLIENT_ID     = () => process.env.GMAIL_CLIENT_ID;
const CLIENT_SECRET = () => process.env.GMAIL_CLIENT_SECRET;
const REFRESH_TOKEN = () => process.env.GMAIL_REFRESH_TOKEN;
const USER_EMAIL    = () => process.env.GMAIL_USER_EMAIL || 'daniel@aeolabs.ai';

// ── Token Management ─────────────────────────────────────────────────────────

let _cachedToken = null;
let _tokenExpiry = 0;

async function getAccessToken() {
  // Return cached token if still valid (with 60s buffer)
  if (_cachedToken && Date.now() < _tokenExpiry - 60_000) {
    return _cachedToken;
  }

  const res = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id: CLIENT_ID(),
      client_secret: CLIENT_SECRET(),
      refresh_token: REFRESH_TOKEN(),
      grant_type: 'refresh_token',
    }),
  });

  const data = await res.json();
  if (data.error) {
    throw new Error(`Gmail token refresh failed: ${data.error_description || data.error}`);
  }

  _cachedToken = data.access_token;
  _tokenExpiry = Date.now() + (data.expires_in || 3600) * 1000;
  return _cachedToken;
}

// ── Gmail API Helpers ────────────────────────────────────────────────────────

async function gmailFetch(accessToken, path, opts = {}) {
  const res = await fetch(`https://gmail.googleapis.com/gmail/v1/users/me${path}`, {
    ...opts,
    headers: {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
      ...(opts.headers || {}),
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Gmail API ${path} (${res.status}): ${text.slice(0, 300)}`);
  }

  return res.json();
}

function decodeBase64(str) {
  return Buffer.from(str.replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf-8');
}

function extractText(parts) {
  let text = '';
  for (const part of (parts || [])) {
    if (part.mimeType === 'text/plain' && part.body?.data) {
      text += decodeBase64(part.body.data);
    } else if (part.parts) {
      text += extractText(part.parts);
    }
  }
  return text;
}

// ── Public API ───────────────────────────────────────────────────────────────

/**
 * List messages matching a Gmail search query
 * @param {string} query - Gmail search query (e.g. 'is:unread in:inbox')
 * @param {number} maxResults - Max messages to return
 * @returns {Array<{id: string, threadId: string}>}
 */
async function listMessages(query, maxResults = 20) {
  const token = await getAccessToken();
  const qs = new URLSearchParams({ q: query, maxResults: String(maxResults) });
  const data = await gmailFetch(token, `/messages?${qs}`);
  return data.messages || [];
}

/**
 * Get full message content by ID
 * @param {string} messageId
 * @returns {object} Parsed message with id, threadId, messageId (RFC822), from, to, subject, date, snippet, body
 */
async function getMessage(messageId) {
  const token = await getAccessToken();
  const msg = await gmailFetch(token, `/messages/${messageId}?format=full`);

  const headers = {};
  for (const h of (msg.payload?.headers || [])) {
    headers[h.name.toLowerCase()] = h.value;
  }

  let body = '';
  if (msg.payload?.body?.data) {
    body = decodeBase64(msg.payload.body.data);
  } else if (msg.payload?.parts) {
    body = extractText(msg.payload.parts);
  }

  return {
    id: msg.id,
    threadId: msg.threadId,
    messageId: headers['message-id'] || '',
    from: headers['from'] || '',
    to: headers['to'] || '',
    subject: headers['subject'] || '(no subject)',
    date: headers['date'] || '',
    snippet: msg.snippet || '',
    body: body.slice(0, 1500),
    labelIds: msg.labelIds || [],
  };
}

/**
 * Send a reply in an existing thread
 * @param {string} messageId - Gmail message ID (for threading)
 * @param {string} threadId - Gmail thread ID
 * @param {string} to - Recipient email
 * @param {string} subject - Email subject (Re: prefix auto-added)
 * @param {string} body - Plain text reply body
 */
async function sendReply(messageId, threadId, to, subject, body) {
  const token = await getAccessToken();
  const fromEmail = USER_EMAIL();

  const subjectLine = subject.startsWith('Re:') ? subject : `Re: ${subject}`;
  const rawHeaders = [
    `From: ${fromEmail}`,
    `To: ${to}`,
    `Subject: ${subjectLine}`,
    `Content-Type: text/plain; charset="UTF-8"`,
    messageId ? `In-Reply-To: ${messageId}` : '',
    messageId ? `References: ${messageId}` : '',
  ].filter(Boolean).join('\r\n');

  const raw = `${rawHeaders}\r\n\r\n${body}`;
  const encoded = Buffer.from(raw).toString('base64url');

  return gmailFetch(token, '/messages/send', {
    method: 'POST',
    body: JSON.stringify({ raw: encoded, threadId }),
  });
}

/**
 * Mark a message as read (remove UNREAD label)
 * @param {string} messageId
 */
async function markAsRead(messageId) {
  const token = await getAccessToken();
  await gmailFetch(token, `/messages/${messageId}/modify`, {
    method: 'POST',
    body: JSON.stringify({ removeLabelIds: ['UNREAD'] }),
  });
}

/**
 * Archive a message (remove INBOX label)
 * @param {string} messageId
 */
async function archiveMessage(messageId) {
  const token = await getAccessToken();
  await gmailFetch(token, `/messages/${messageId}/modify`, {
    method: 'POST',
    body: JSON.stringify({ removeLabelIds: ['INBOX', 'UNREAD'] }),
  });
}

/**
 * Get unread replies to outreach emails
 * Filters for: unread, in inbox, with "Re:" subject (replies to our outreach)
 * @param {number} maxResults
 * @returns {Array<object>} Full message objects
 */
async function getUnreadOutreachReplies(maxResults = 25) {
  // Fetch unread inbox messages — we filter further after classification
  const messages = await listMessages('is:unread in:inbox', maxResults);
  return messages;
}

/**
 * Get recent inbox messages (regardless of read status) from the last N minutes.
 * Excludes messages sent by us (from: daniel@aeolabs.ai).
 * Used as fallback when emails are read before gmail-poll gets to them.
 * @param {number} minutesBack - How many minutes back to look
 * @param {number} maxResults
 * @returns {Array<{id: string, threadId: string}>}
 */
async function getRecentInboxMessages(minutesBack = 10, maxResults = 50) {
  const since = new Date(Date.now() - minutesBack * 60 * 1000);
  const afterDate = `${since.getFullYear()}/${String(since.getMonth() + 1).padStart(2, '0')}/${String(since.getDate()).padStart(2, '0')}`;
  const userEmail = USER_EMAIL();
  // Get inbox messages from recent time window, excluding our own sends
  const messages = await listMessages(`in:inbox after:${afterDate} -from:${userEmail}`, maxResults);
  return messages;
}

/**
 * Add a label to a message (create label if needed)
 */
async function addLabel(messageId, labelName) {
  const token = await getAccessToken();
  // Get or create the label
  const labelsRes = await gmailFetch(token, '/labels');
  let label = (labelsRes.labels || []).find(l => l.name === labelName);
  if (!label) {
    label = await gmailFetch(token, '/labels', {
      method: 'POST',
      body: JSON.stringify({ name: labelName, labelListVisibility: 'labelHide', messageListVisibility: 'hide' }),
    });
  }
  if (label?.id) {
    await gmailFetch(token, `/messages/${messageId}/modify`, {
      method: 'POST',
      body: JSON.stringify({ addLabelIds: [label.id] }),
    });
  }
  return label?.id;
}

/**
 * Check if a message has a specific label
 */
async function hasLabel(messageId, labelName) {
  const token = await getAccessToken();
  const labelsRes = await gmailFetch(token, '/labels');
  const label = (labelsRes.labels || []).find(l => l.name === labelName);
  if (!label) return false;
  const msg = await gmailFetch(token, `/messages/${messageId}?format=minimal`);
  return (msg.labelIds || []).includes(label.id);
}

/**
 * Get count of unread messages (for digest)
 */
async function getUnreadCount() {
  const token = await getAccessToken();
  const data = await gmailFetch(token, '/messages?q=is:unread+in:inbox&maxResults=1');
  return data.resultSizeEstimate || 0;
}

/**
 * Get messages sent today (for digest — count auto-replies)
 */
async function getSentToday() {
  const today = new Date();
  const dateStr = `${today.getFullYear()}/${String(today.getMonth() + 1).padStart(2, '0')}/${String(today.getDate()).padStart(2, '0')}`;
  const messages = await listMessages(`in:sent after:${dateStr}`, 50);
  return messages;
}

/**
 * Get recent messages from last N days
 */
async function getRecentMessages(daysBack = 1, maxResults = 50) {
  const since = new Date();
  since.setDate(since.getDate() - daysBack);
  const afterDate = `${since.getFullYear()}/${String(since.getMonth() + 1).padStart(2, '0')}/${String(since.getDate()).padStart(2, '0')}`;
  return listMessages(`in:inbox after:${afterDate}`, maxResults);
}

module.exports = {
  getAccessToken,
  listMessages,
  getMessage,
  sendReply,
  markAsRead,
  archiveMessage,
  getUnreadOutreachReplies,
  getRecentInboxMessages,
  addLabel,
  hasLabel,
  getUnreadCount,
  getSentToday,
  getRecentMessages,
};
