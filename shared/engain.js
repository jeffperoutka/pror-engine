/**
 * Engain API Connector for PROR Engine
 *
 * Handles all communication with the Engain Reddit execution platform.
 * Rate limited to 30 req/min with automatic queuing and retry.
 *
 * Auth: X-API-Key header (env: ENGAIN_API_KEY)
 * Base URL: https://api.engain.io/api/v1
 */

const BASE_URL = 'https://api.engain.io/api/v1';

// ── Rate Limiter (30 req/min) ──

const RATE_LIMIT = 30;
const RATE_WINDOW = 60 * 1000; // 1 minute
const timestamps = [];
const queue = [];
let processing = false;

function getApiKey() {
  const key = process.env.ENGAIN_API_KEY;
  if (!key) throw new Error('ENGAIN_API_KEY not set');
  return key;
}

function getProjectId() {
  const id = process.env.ENGAIN_PROJECT_ID;
  if (!id) throw new Error('ENGAIN_PROJECT_ID not set');
  return id;
}

/**
 * Wait until we have capacity within the rate limit window.
 * Returns a promise that resolves when a slot is available.
 */
function waitForSlot() {
  return new Promise((resolve) => {
    const now = Date.now();
    // Purge timestamps older than the window
    while (timestamps.length && timestamps[0] <= now - RATE_WINDOW) {
      timestamps.shift();
    }
    if (timestamps.length < RATE_LIMIT) {
      timestamps.push(now);
      resolve();
    } else {
      // Wait until the oldest timestamp expires
      const waitMs = timestamps[0] + RATE_WINDOW - now + 50; // 50ms buffer
      setTimeout(() => {
        while (timestamps.length && timestamps[0] <= Date.now() - RATE_WINDOW) {
          timestamps.shift();
        }
        timestamps.push(Date.now());
        resolve();
      }, waitMs);
    }
  });
}

/**
 * Process queued requests sequentially with rate limiting.
 */
async function processQueue() {
  if (processing) return;
  processing = true;
  while (queue.length > 0) {
    const { execute, resolve, reject } = queue.shift();
    try {
      await waitForSlot();
      const result = await execute();
      resolve(result);
    } catch (err) {
      reject(err);
    }
  }
  processing = false;
}

/**
 * Enqueue a request to be executed within rate limits.
 */
function enqueue(executeFn) {
  return new Promise((resolve, reject) => {
    queue.push({ execute: executeFn, resolve, reject });
    processQueue();
  });
}

// ── HTTP Layer with Retries ──

const MAX_RETRIES = 3;
const RETRY_DELAYS = [1000, 3000, 8000]; // Exponential-ish backoff

async function engainFetch(method, path, body = null) {
  const url = `${BASE_URL}${path}`;

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const opts = {
      method,
      headers: {
        'X-API-Key': getApiKey(),
        'Content-Type': 'application/json',
      },
    };
    if (body) opts.body = JSON.stringify(body);

    try {
      const resp = await fetch(url, opts);

      // Rate limited by Engain — retry after delay
      if (resp.status === 429 && attempt < MAX_RETRIES) {
        const retryAfter = parseInt(resp.headers.get('retry-after') || '5') * 1000;
        console.warn(`[Engain] 429 rate limited on ${method} ${path}, retrying in ${retryAfter}ms`);
        await new Promise(r => setTimeout(r, retryAfter));
        continue;
      }

      // Server error — retry
      if (resp.status >= 500 && attempt < MAX_RETRIES) {
        console.warn(`[Engain] ${resp.status} on ${method} ${path}, retry ${attempt + 1}/${MAX_RETRIES}`);
        await new Promise(r => setTimeout(r, RETRY_DELAYS[attempt]));
        continue;
      }

      const data = await resp.json();

      if (!resp.ok) {
        const msg = data?.message || data?.error || `HTTP ${resp.status}`;
        const err = new Error(`Engain ${method} ${path}: ${msg}`);
        err.status = resp.status;
        err.data = data;
        throw err;
      }

      return data;
    } catch (err) {
      // Network errors — retry
      if (err.status === undefined && attempt < MAX_RETRIES) {
        console.warn(`[Engain] Network error on ${method} ${path}: ${err.message}, retry ${attempt + 1}/${MAX_RETRIES}`);
        await new Promise(r => setTimeout(r, RETRY_DELAYS[attempt]));
        continue;
      }
      throw err;
    }
  }
}

/**
 * Rate-limited wrapper around engainFetch.
 * All public methods go through this to respect 30 req/min.
 */
function rateLimitedFetch(method, path, body = null) {
  return enqueue(() => engainFetch(method, path, body));
}

// ── Thread Validation ──

/**
 * Check if a Reddit thread is archived/locked before posting.
 * Reddit auto-archives threads after ~180 days.
 * Returns { archived, age, locked, error } or throws on network failure.
 */
async function checkThreadStatus(threadUrl) {
  const jsonUrl = threadUrl
    .replace('www.reddit.com', 'old.reddit.com')
    .replace(/\/$/, '') + '.json?limit=1';

  try {
    const resp = await fetch(jsonUrl, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; ProrEngine/1.0)' },
    });
    if (!resp.ok) {
      return { archived: null, error: `HTTP ${resp.status}` };
    }
    const data = await resp.json();
    const post = data?.[0]?.data?.children?.[0]?.data;
    if (!post) return { archived: null, error: 'Could not parse thread data' };

    const ageDays = Math.floor((Date.now() / 1000 - post.created_utc) / 86400);
    return {
      archived: post.archived || ageDays > 180,
      locked: post.locked,
      age: ageDays,
      subreddit: post.subreddit,
      title: post.title,
    };
  } catch (err) {
    return { archived: null, error: err.message };
  }
}

// ── Task Creation ──

/**
 * Post a comment on a Reddit thread.
 * Checks thread archive status first — rejects if archived.
 *
 * @param {string} threadUrl - Reddit post URL
 * @param {string} content - Comment text
 * @param {string} [scheduleAt] - ISO 8601 datetime (optional)
 * @param {object} [opts] - { skipArchiveCheck: false }
 */
async function createComment(threadUrl, content, scheduleAt, opts = {}) {
  if (!opts.skipArchiveCheck) {
    const status = await checkThreadStatus(threadUrl);
    if (status.archived) {
      const err = new Error(`Thread is archived (${status.age} days old): ${threadUrl}`);
      err.code = 'THREAD_ARCHIVED';
      err.threadStatus = status;
      throw err;
    }
    if (status.locked) {
      const err = new Error(`Thread is locked: ${threadUrl}`);
      err.code = 'THREAD_LOCKED';
      err.threadStatus = status;
      throw err;
    }
  }

  const body = {
    projectId: getProjectId(),
    url: threadUrl,
    content,
  };
  if (scheduleAt) body.schedule_at = scheduleAt;
  return rateLimitedFetch('POST', '/tasks/comment', body);
}

/**
 * Create a new Reddit post.
 * @param {string} subreddit - Subreddit name or URL (e.g., "technology", "r/technology", or full URL)
 * @param {string} title - Post title
 * @param {string} content - Post body text
 * @param {string} [scheduleAt] - ISO 8601 datetime (optional)
 */
async function createPost(subreddit, title, content, scheduleAt) {
  // Accept either subreddit name or full URL
  let subredditUrl;
  if (subreddit.startsWith('http')) {
    subredditUrl = subreddit;
  } else {
    const subName = subreddit.replace(/^r\//, '').replace(/^\/r\//, '').trim();
    subredditUrl = `https://reddit.com/r/${subName}`;
  }

  const body = {
    projectId: getProjectId(),
    subredditUrl,
    postTitle: title,
    content,
  };
  if (scheduleAt) body.schedule_at = scheduleAt;
  return rateLimitedFetch('POST', '/tasks/post', body);
}

/**
 * Reply to a Reddit comment.
 * @param {string} commentUrl - Reddit comment permalink URL
 * @param {string} content - Reply text
 * @param {string} [scheduleAt] - ISO 8601 datetime (optional)
 */
async function createReply(commentUrl, content, scheduleAt) {
  const body = {
    projectId: getProjectId(),
    url: commentUrl,
    content,
  };
  if (scheduleAt) body.schedule_at = scheduleAt;
  return rateLimitedFetch('POST', '/tasks/reply', body);
}

/**
 * Submit upvotes on a Reddit post or comment.
 * @param {string} targetUrl - Reddit post or comment URL
 * @param {number} [count=3] - Number of upvotes (1-2000)
 * @param {string} [scheduleAt] - ISO 8601 datetime (optional)
 */
async function createUpvote(targetUrl, count = 3, scheduleAt) {
  const body = {
    projectId: getProjectId(),
    url: targetUrl,
    count,
  };
  if (scheduleAt) body.schedule_at = scheduleAt;
  return rateLimitedFetch('POST', '/tasks/upvote', body);
}

// ── Task Tracking ──

/**
 * Get a single task by ID.
 * @param {string} taskId
 */
async function getTask(taskId) {
  return rateLimitedFetch('GET', `/tasks/${taskId}?projectId=${getProjectId()}`);
}

/**
 * List tasks with optional filters.
 * @param {object} [filters] - { status, type, limit, offset }
 */
async function listTasks(filters = {}) {
  const params = new URLSearchParams();
  params.set('projectId', getProjectId());
  if (filters.status) params.set('status', filters.status);
  if (filters.type) params.set('type', filters.type);
  if (filters.limit) params.set('limit', String(filters.limit));
  if (filters.offset) params.set('offset', String(filters.offset));
  const qs = params.toString();
  return rateLimitedFetch('GET', `/tasks?${qs}`);
}

module.exports = {
  checkThreadStatus,
  createComment,
  createPost,
  createReply,
  createUpvote,
  getTask,
  listTasks,
  getProjectId,
};
