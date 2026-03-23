/**
 * Slack Slash Commands handler
 * Routes: /links, /reddit, /qa, /sprint, /clients, /status, /finances
 */
const slack = require('../../shared/slack');
const brain = require('../../shared/brain');

// Agent handlers — lazy loaded
const agents = {
  'link-builder': () => require('../../agents/link-builder'),
  'reddit': () => require('../../agents/reddit-strategist'),
  'qa': () => require('../../agents/qa-checker'),
  'orchestrator': () => require('../../agents/orchestrator'),
};

module.exports = async (req, res) => {
  // Parse the slash command
  const { command, text, channel_id, user_id, response_url, trigger_id } = req.body;

  // Acknowledge immediately
  res.status(200).json({
    response_type: 'in_channel',
    text: `⏳ Processing \`${command} ${text || ''}\`...`,
  });

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
};
