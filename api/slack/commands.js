/**
 * Slack Slash Commands handler
 * Routes: /links, /reddit, /qa, /initiation, /clients, /current-status, /finances
 */
const { waitUntil } = require('@vercel/functions');
const slack = require('../../shared/slack');
const brain = require('../../shared/brain');

// Agent handlers — lazy loaded
const agents = {
  'link-builder': () => require('../../agents/link-builder'),
  'reddit': () => require('../../agents/reddit-strategist'),
  'qa': () => require('../../agents/qa-checker'),
  'orchestrator': () => require('../../agents/orchestrator'),
};

async function processCommand(body) {
  const { command, text, channel_id, user_id, trigger_id, response_url } = body;

  try {
    // Route through brain
    const routing = await brain.handleCommand(command, text, {
      channel: channel_id,
      user: user_id,
    });

    // Get the agent handler
    const agentLoader = agents[routing.agent];
    if (!agentLoader) {
      await slack.post(channel_id, `❌ Unknown agent: ${routing.agent}`);
      return;
    }

    const agent = agentLoader();

    // Execute the agent
    await agent.execute({
      ...routing.args,
      channel: channel_id,
      user: user_id,
      trigger_id,
      response_url,
    });

  } catch (err) {
    console.error(`Command ${command} error:`, err);
    await slack.post(channel_id, `❌ Error executing \`${command}\`: ${err.message}`);
  }
}

module.exports = async (req, res) => {
  // Use waitUntil to keep the function alive after responding
  waitUntil(processCommand(req.body));

  // Acknowledge immediately
  res.status(200).json({
    response_type: 'in_channel',
    text: `⏳ Processing \`${req.body.command} ${req.body.text || ''}\`...`,
  });
};
