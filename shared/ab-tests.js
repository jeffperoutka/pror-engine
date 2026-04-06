/**
 * ab-tests.js — Lightweight A/B testing for outreach campaigns
 *
 * Tests email copy variants (subject lines, body), reply strategies,
 * and negotiation approaches. Tracks results in Airtable.
 *
 * How it works:
 *   1. Define a test with variants A and B
 *   2. When creating campaigns, prospects are randomly split 50/50
 *   3. Each campaign is tagged with its variant
 *   4. After enough sends, compare open rate, reply rate, negotiation success
 *   5. Winner becomes the new default
 *
 * Tests run automatically — no manual intervention needed. Results
 * posted to Slack weekly and when a test reaches statistical significance.
 */

const airtable = require('./airtable');

// ── Active Test Definitions ─────────────────────────────────────────────────
// Add new tests here. Each test has two variants (A/B).
// The system will automatically split traffic and track results.

const ACTIVE_TESTS = {
  // Test 1: Subject line — niche-specific vs generic
  'subject-niche-v2': {
    metric: 'open_rate',
    minSampleSize: 30, // lowered from 50 to get results faster
    variants: {
      A: {
        label: 'Niche topic + site name (new default)',
        subjectTemplate: '{{params.NICHE}} piece for {{params.SITE_NAME}}',
      },
      B: {
        label: 'Curiosity hook — no niche reveal',
        subjectTemplate: 'Thought this might fit {{params.SITE_NAME}}',
      },
    },
  },

  // Test 2: Opening line approach
  'opening-line-v1': {
    metric: 'reply_rate',
    minSampleSize: 30,
    variants: {
      A: {
        label: 'Compliment opener (reference article)',
        bodyPrefix: 'Your piece on {{params.ARTICLE_TITLE}} was a great read.',
      },
      B: {
        label: 'Direct value — skip flattery',
        bodyPrefix: 'I have a content piece that would be a great fit for your readers.',
      },
    },
  },

  // Test 3: Negotiation opening offer
  'negotiation-anchor-v1': {
    metric: 'negotiation_success',
    minSampleSize: 20,
    variants: {
      A: {
        label: '50% anchor',
        offerPct: 0.50,
      },
      B: {
        label: '40% anchor (more aggressive)',
        offerPct: 0.40,
      },
    },
  },

  // Test 4: Follow-up timing
  'followup-timing-v1': {
    metric: 'reply_rate',
    minSampleSize: 30,
    variants: {
      A: {
        label: 'Standard (Day 3, 7, 14)',
        delays: [3, 7, 14],
      },
      B: {
        label: 'Faster (Day 2, 5, 10)',
        delays: [2, 5, 10],
      },
    },
  },

  // Test 5: Sender name — "Josh" vs "Josh from [Brand]"
  'sender-name-v1': {
    metric: 'open_rate',
    minSampleSize: 30,
    variants: {
      A: {
        label: 'First name only: Josh',
        senderName: 'Josh',
      },
      B: {
        label: 'Name + role: Josh | Content Partnerships',
        senderName: 'Josh | Content Partnerships',
      },
    },
  },

  // Test 6: CTA style — question vs statement
  'cta-style-v1': {
    metric: 'reply_rate',
    minSampleSize: 30,
    variants: {
      A: {
        label: 'Question CTA',
        ctaSuffix: 'Would you be open to a contributed article?',
      },
      B: {
        label: 'Soft assumption CTA',
        ctaSuffix: 'Happy to send over a few topic ideas if you\'re interested.',
      },
    },
  },
};

// ── Variant Assignment ──────────────────────────────────────────────────────

/**
 * Deterministically assign a variant based on email hash.
 * This ensures the same prospect always gets the same variant
 * even if the system reruns.
 */
function assignVariant(testName, email) {
  let hash = 0;
  const str = `${testName}:${email}`;
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) - hash + str.charCodeAt(i)) | 0;
  }
  return Math.abs(hash) % 2 === 0 ? 'A' : 'B';
}

/**
 * Get the variant config for a prospect in a given test.
 * Returns { variant: 'A'|'B', config: {...} } or null if test not active.
 */
function getVariant(testName, email) {
  const test = ACTIVE_TESTS[testName];
  if (!test) return null;

  const variant = assignVariant(testName, email);
  return {
    variant,
    config: test.variants[variant],
    testName,
    metric: test.metric,
  };
}

/**
 * Get all active test assignments for a prospect.
 * Returns map of testName -> { variant, config }.
 */
function getAllVariants(email) {
  const assignments = {};
  for (const testName of Object.keys(ACTIVE_TESTS)) {
    assignments[testName] = getVariant(testName, email);
  }
  return assignments;
}

// ── Result Tracking ─────────────────────────────────────────────────────────

/**
 * Record a test event (send, open, reply, negotiation_success).
 * Called from brevo campaign callbacks and gmail-poll.
 */
async function recordEvent(testName, variant, email, event) {
  // event: 'sent', 'opened', 'replied', 'negotiation_success', 'negotiation_fail'
  try {
    await airtable.logABTest({
      testName,
      variant,
      metric: event,
      sampleSize: 1,
      result: 1,
      status: 'running',
    });
  } catch (err) {
    console.error(`[ab-test] Failed to record event: ${err.message}`);
  }
}

/**
 * Calculate results for a test. Returns winner or 'inconclusive'.
 */
async function getTestResults(testName) {
  const test = ACTIVE_TESTS[testName];
  if (!test) return null;

  try {
    const allRecords = await airtable.getABTests('running');
    const testRecords = allRecords.filter(r => r.TestName === testName);

    const stats = { A: { sent: 0, opened: 0, replied: 0, success: 0 }, B: { sent: 0, opened: 0, replied: 0, success: 0 } };

    for (const r of testRecords) {
      const v = r.Variant;
      if (!stats[v]) continue;
      const metric = r.Metric;
      if (metric === 'sent') stats[v].sent += r.Result || 1;
      else if (metric === 'opened') stats[v].opened += r.Result || 1;
      else if (metric === 'replied') stats[v].replied += r.Result || 1;
      else if (metric === 'negotiation_success') stats[v].success += r.Result || 1;
    }

    // Calculate rates
    const rateA = stats.A.sent > 0 ? (test.metric === 'open_rate' ? stats.A.opened / stats.A.sent : test.metric === 'reply_rate' ? stats.A.replied / stats.A.sent : stats.A.success / stats.A.replied) : 0;
    const rateB = stats.B.sent > 0 ? (test.metric === 'open_rate' ? stats.B.opened / stats.B.sent : test.metric === 'reply_rate' ? stats.B.replied / stats.B.sent : stats.B.success / stats.B.replied) : 0;

    const totalSamples = Math.min(stats.A.sent, stats.B.sent);
    const hasEnoughData = totalSamples >= test.minSampleSize;

    let winner = 'inconclusive';
    if (hasEnoughData) {
      // Simple: >10% relative improvement = winner
      const improvement = rateA > 0 ? (rateB - rateA) / rateA : (rateB > 0 ? 1 : 0);
      if (improvement > 0.10) winner = 'B';
      else if (improvement < -0.10) winner = 'A';
    }

    return {
      testName,
      metric: test.metric,
      stats,
      rateA: (rateA * 100).toFixed(1) + '%',
      rateB: (rateB * 100).toFixed(1) + '%',
      totalSamples,
      minRequired: test.minSampleSize,
      hasEnoughData,
      winner,
      variantALabel: test.variants.A.label,
      variantBLabel: test.variants.B.label,
    };
  } catch (err) {
    console.error(`[ab-test] getTestResults failed: ${err.message}`);
    return null;
  }
}

/**
 * Format test results for Slack posting.
 */
function formatResultsForSlack(results) {
  if (!results) return 'No test data available';

  const statusEmoji = results.winner === 'inconclusive'
    ? ':hourglass:'
    : results.winner === 'A' ? ':a:' : ':b:';

  return [
    `${statusEmoji} *${results.testName}* (${results.metric})`,
    `  Variant A (${results.variantALabel}): ${results.rateA} (n=${results.stats.A.sent})`,
    `  Variant B (${results.variantBLabel}): ${results.rateB} (n=${results.stats.B.sent})`,
    `  ${results.hasEnoughData ? `*Winner: ${results.winner}*` : `Need ${results.minRequired - results.totalSamples} more samples`}`,
  ].join('\n');
}

module.exports = {
  ACTIVE_TESTS,
  assignVariant,
  getVariant,
  getAllVariants,
  recordEvent,
  getTestResults,
  formatResultsForSlack,
};
