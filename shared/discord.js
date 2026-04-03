/**
 * Shared Discord utilities for PROR Engine
 * Drop-in companion to slack.js — all crons can dual-post to Discord
 * Uses Discord webhooks (no bot token needed, just webhook URLs)
 */

// Channel map — mirrors slack.js CHANNELS but returns Discord webhook URLs
const CHANNELS = {
  command: () => process.env.DISCORD_WEBHOOK_COMMAND,
  'daily-brief': () => process.env.DISCORD_WEBHOOK_DAILY_BRIEF,
  'weekly-report': () => process.env.DISCORD_WEBHOOK_WEEKLY_REPORT,
  'system-ops': () => process.env.DISCORD_WEBHOOK_SYSTEM_OPS,
  links: () => process.env.DISCORD_WEBHOOK_LINKS,
  reddit: () => process.env.DISCORD_WEBHOOK_REDDIT,
  clients: () => process.env.DISCORD_WEBHOOK_CLIENTS,
  qa: () => process.env.DISCORD_WEBHOOK_QA,
  finances: () => process.env.DISCORD_WEBHOOK_FINANCES,
};

/**
 * Post a plain text message via Discord webhook.
 * Automatically splits messages over 2000 chars into multiple posts at line boundaries.
 */
async function post(webhookUrl, text) {
  if (!webhookUrl) {
    console.warn('[discord] No webhook URL provided, skipping post');
    return null;
  }

  // Split into chunks that fit within Discord's 2000-char limit
  const chunks = splitText(text, 2000);
  let lastResult = null;

  for (let i = 0; i < chunks.length; i++) {
    const res = await fetch(webhookUrl + '?wait=true', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content: chunks[i] }),
    });

    if (!res.ok) {
      const err = await res.text();
      console.error('[discord] Post failed:', res.status, err);
      return null;
    }
    lastResult = await res.json();

    // Rate limit safety between chunks
    if (i < chunks.length - 1) {
      await new Promise((r) => setTimeout(r, 500));
    }
  }

  return lastResult;
}

/**
 * Post rich embeds via Discord webhook
 * Converts Slack Block Kit blocks to Discord embeds
 */
async function postBlocks(webhookUrl, blocks, text = '') {
  if (!webhookUrl) {
    console.warn('[discord] No webhook URL provided, skipping postBlocks');
    return null;
  }

  const embeds = convertBlocksToEmbeds(blocks);

  // Discord allows max 10 embeds per message
  // If we have more, batch them
  const batches = [];
  for (let i = 0; i < embeds.length; i += 10) {
    batches.push(embeds.slice(i, i + 10));
  }

  let lastResult = null;
  for (let i = 0; i < batches.length; i++) {
    const payload = { embeds: batches[i] };
    // Include text content only on first message
    if (i === 0 && text) {
      // Strip Slack-specific markers like <!channel>
      payload.content = text
        .replace(/<!channel>/g, '@everyone')
        .replace(/<!here>/g, '@here')
        .slice(0, 2000);
    }

    const res = await fetch(webhookUrl + '?wait=true', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      const err = await res.text();
      console.error('[discord] PostBlocks failed:', res.status, err);
    } else {
      lastResult = await res.json();
    }

    // Rate limit safety between batches
    if (i < batches.length - 1) {
      await new Promise((r) => setTimeout(r, 500));
    }
  }

  return lastResult;
}

/**
 * Convert Slack Block Kit blocks to Discord embeds
 */
function convertBlocksToEmbeds(blocks) {
  if (!blocks || !Array.isArray(blocks)) return [];

  const embeds = [];
  let currentEmbed = { color: 0x1b2a4a }; // Navy default
  let descParts = [];

  for (const block of blocks) {
    switch (block.type) {
      case 'header': {
        // Flush previous embed if it has content
        if (descParts.length > 0) {
          currentEmbed.description = descParts.join('\n');
          embeds.push(currentEmbed);
          currentEmbed = { color: 0x1b2a4a };
          descParts = [];
        }
        currentEmbed.title = extractText(block.text);
        break;
      }

      case 'section': {
        const text = extractText(block.text);
        if (text) descParts.push(slackToDiscordMarkdown(text));

        // Handle section fields (Slack puts multiple mrkdwn fields side by side)
        if (block.fields) {
          for (const field of block.fields) {
            descParts.push(slackToDiscordMarkdown(extractText(field)));
          }
        }
        break;
      }

      case 'context': {
        const elements = (block.elements || [])
          .map((el) => extractText(el))
          .filter(Boolean);
        if (elements.length > 0) {
          descParts.push(
            '_' + elements.map(slackToDiscordMarkdown).join(' · ') + '_'
          );
        }
        break;
      }

      case 'divider': {
        descParts.push('───────────────────────');
        break;
      }

      case 'actions': {
        // Slack interactive buttons — convert to text links or labels
        const labels = (block.elements || [])
          .map((el) => `\`${extractText(el.text) || el.value || 'action'}\``)
          .filter(Boolean);
        if (labels.length > 0) {
          descParts.push(labels.join('  '));
        }
        break;
      }

      default:
        break;
    }
  }

  // Flush remaining content
  if (descParts.length > 0 || currentEmbed.title) {
    if (descParts.length > 0) {
      currentEmbed.description = descParts.join('\n');
    }
    embeds.push(currentEmbed);
  }

  // Discord embed description max is 4096 chars — split if needed
  const finalEmbeds = [];
  for (const embed of embeds) {
    if (embed.description && embed.description.length > 4096) {
      const chunks = splitText(embed.description, 4096);
      for (let i = 0; i < chunks.length; i++) {
        finalEmbeds.push({
          ...embed,
          title: i === 0 ? embed.title : undefined,
          description: chunks[i],
        });
      }
    } else {
      finalEmbeds.push(embed);
    }
  }

  return finalEmbeds;
}

/**
 * Extract plain text from Slack text objects
 */
function extractText(textObj) {
  if (!textObj) return '';
  if (typeof textObj === 'string') return textObj;
  return textObj.text || '';
}

/**
 * Convert Slack mrkdwn to Discord markdown
 * Slack uses *bold*, _italic_, ~strike~, <url|label>
 * Discord uses **bold**, *italic*, ~~strike~~, [label](url)
 */
function slackToDiscordMarkdown(text) {
  if (!text) return '';

  return (
    text
      // Slack links: <url|label> → [label](url)
      .replace(/<(https?:\/\/[^|>]+)\|([^>]+)>/g, '[$2]($1)')
      // Slack bare links: <url> → url
      .replace(/<(https?:\/\/[^>]+)>/g, '$1')
      // Slack user mentions: <@U123> → @user
      .replace(/<@(\w+)>/g, '@$1')
      // Slack channel mentions: <#C123|name> → #name
      .replace(/<#\w+\|([^>]+)>/g, '#$1')
      // Slack special: <!channel> → @everyone, <!here> → @here
      .replace(/<!channel>/g, '@everyone')
      .replace(/<!here>/g, '@here')
      // Slack bold: *text* → **text** (but not if already **)
      .replace(/(?<!\*)\*([^*]+)\*(?!\*)/g, '**$1**')
      // Slack strikethrough: ~text~ → ~~text~~
      .replace(/(?<!~)~([^~]+)~(?!~)/g, '~~$1~~')
    // Note: _italic_ is the same in both Slack and Discord
  );
}

/**
 * Split long text into chunks at line boundaries
 */
function splitText(text, maxLen) {
  const chunks = [];
  const lines = text.split('\n');
  let current = '';

  for (const line of lines) {
    if (current.length + line.length + 1 > maxLen) {
      chunks.push(current);
      current = line;
    } else {
      current += (current ? '\n' : '') + line;
    }
  }
  if (current) chunks.push(current);
  return chunks;
}

/**
 * Post to Discord only if webhook is configured, otherwise silently skip.
 * This is the main entry point for crons during migration.
 */
async function postIfConfigured(channelKey, text, blocks = null) {
  const webhookUrl = CHANNELS[channelKey]?.();
  if (!webhookUrl) return null;

  if (blocks) {
    return postBlocks(webhookUrl, blocks, text);
  }
  return post(webhookUrl, text);
}

module.exports = {
  CHANNELS,
  post,
  postBlocks,
  postIfConfigured,
  convertBlocksToEmbeds,
  slackToDiscordMarkdown,
};
