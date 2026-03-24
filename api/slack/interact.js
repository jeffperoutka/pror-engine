/**
 * Slack Interactions handler
 * Handles: button clicks, modal submissions, action callbacks
 */
const { waitUntil } = require('@vercel/functions');
const slack = require('../../shared/slack');
const airtable = require('../../shared/airtable');

async function processInteraction(payload) {
  try {
    const type = payload.type;
    const user = payload.user?.id;
    const channel = payload.channel?.id;

    if (type === 'block_actions') {
      for (const action of payload.actions || []) {
        await handleAction(action, {
          channel,
          user,
          message_ts: payload.message?.ts,
          thread_ts: payload.message?.thread_ts,
          trigger_id: payload.trigger_id,
        });
      }
    }

    if (type === 'view_submission') {
      await handleModal(payload.view, { user, trigger_id: payload.trigger_id });
    }

  } catch (err) {
    console.error('Interaction error:', err);
  }
}

module.exports = async (req, res) => {
  // Slack sends interactions as a URL-encoded payload
  let payload;
  try {
    payload = JSON.parse(req.body.payload || '{}');
  } catch {
    return res.status(400).json({ error: 'Invalid payload' });
  }

  // Use waitUntil to keep the function alive after responding
  waitUntil(processInteraction(payload));

  // Acknowledge immediately
  res.status(200).json({ ok: true });
};

async function handleAction(action, context) {
  const actionId = action.action_id;
  const value = action.value;

  // Route based on action_id prefix
  // Format: agent:action:data
  const [agent, act, ...data] = actionId.split(':');

  switch (agent) {
    case 'links': {
      const linkAgent = require('../../agents/link-builder');
      await linkAgent.handleAction(act, data.join(':'), context);
      break;
    }
    case 'reddit': {
      const redditAgent = require('../../agents/reddit-strategist');
      await redditAgent.handleAction(act, data.join(':'), context);
      break;
    }
    case 'qa': {
      const qaAgent = require('../../agents/qa-checker');
      await qaAgent.handleAction(act, data.join(':'), context);
      break;
    }
    case 'approve':
    case 'reject': {
      // Generic approve/reject for any agent
      const targetChannel = context.channel;
      const emoji = agent === 'approve' ? 'white_check_mark' : 'x';
      await slack.react(targetChannel, context.message_ts, emoji);
      await slack.post(targetChannel, `${agent === 'approve' ? '✅ Approved' : '❌ Rejected'} by <@${context.user}>`, {
        thread_ts: context.thread_ts || context.message_ts,
      });
      break;
    }
    default: {
      // ── Negotiation feedback buttons (from gmail-poll) ──
      if (actionId.startsWith('feedback_')) {
        await handleNegotiationFeedback(action, context);
        break;
      }
      console.log('Unknown action:', actionId);
    }
  }
}

/**
 * Handle negotiation feedback buttons from gmail-poll.
 * One tap = feedback logged. Zero friction.
 */
async function handleNegotiationFeedback(action, context) {
  try {
    const data = JSON.parse(action.value || '{}');
    const { domain, round, action: feedback } = data;

    const feedbackLabels = {
      good: ':thumbsup: Good reply',
      bad: ':thumbsdown: Bad reply — will adjust',
      too_high: ':money_with_wings: Offered too high — will go lower next time',
      too_low: ':chart_with_downwards_trend: Offered too low — risked losing the deal',
    };

    const label = feedbackLabels[feedback] || feedback;

    // Log feedback to Airtable Negotiations table
    const history = await airtable.getNegotiationHistory(domain);
    const latestNeg = history[history.length - 1];
    if (latestNeg?.id) {
      await airtable.updateNegotiation(latestNeg.id, {
        JeffReviewed: true,
        JeffFeedback: feedback,
        FeedbackDate: new Date().toISOString(),
      });
    }

    // Acknowledge in thread
    await slack.post(context.channel, `${label} — logged for *${domain}* round ${round}`, {
      thread_ts: context.message_ts,
    });

    // React to original message
    const emoji = feedback === 'good' ? 'white_check_mark' : feedback === 'bad' ? 'x' : 'eyes';
    await slack.react(context.channel, context.message_ts, emoji);

  } catch (err) {
    console.error('[interact] Feedback handler error:', err.message);
  }
}

async function handleModal(view, context) {
  const callbackId = view.callback_id;
  const values = view.state?.values || {};

  // Route based on callback_id prefix
  const [agent, ...rest] = callbackId.split(':');

  switch (agent) {
    case 'links': {
      const linkAgent = require('../../agents/link-builder');
      await linkAgent.handleModal(rest.join(':'), values, context);
      break;
    }
    case 'reddit': {
      const redditAgent = require('../../agents/reddit-strategist');
      await redditAgent.handleModal(rest.join(':'), values, context);
      break;
    }
    default:
      console.log('Unknown modal:', callbackId);
  }
}
