/**
 * Shared AI module — Claude API wrapper for all agents
 * Uses Anthropic SDK directly
 */
const Anthropic = require('@anthropic-ai/sdk');

let _client;
function getClient() {
  if (!_client) _client = new Anthropic();
  return _client;
}

/**
 * Simple text completion
 * @param {string} prompt - User message
 * @param {string} system - System prompt
 * @param {object} opts - Optional: model, maxTokens, temperature
 */
async function complete(prompt, system = '', opts = {}) {
  const client = getClient();
  const response = await client.messages.create({
    model: opts.model || 'claude-sonnet-4-20250514',
    max_tokens: opts.maxTokens || 4096,
    temperature: opts.temperature ?? 0.7,
    system: system || undefined,
    messages: [{ role: 'user', content: prompt }],
  });
  return response.content[0].text;
}

/**
 * Multi-turn conversation
 * @param {Array} messages - Array of {role, content} objects
 * @param {string} system - System prompt
 * @param {object} opts - Optional params
 */
async function chat(messages, system = '', opts = {}) {
  const client = getClient();
  const response = await client.messages.create({
    model: opts.model || 'claude-sonnet-4-20250514',
    max_tokens: opts.maxTokens || 4096,
    temperature: opts.temperature ?? 0.7,
    system: system || undefined,
    messages,
  });
  return response.content[0].text;
}

/**
 * Structured output — ask Claude and parse JSON from response
 * @param {string} prompt
 * @param {string} system
 * @param {object} opts
 */
async function json(prompt, system = '', opts = {}) {
  const systemWithFormat = (system ? system + '\n\n' : '') +
    'IMPORTANT: Respond ONLY with valid JSON. No markdown, no explanation, no code blocks. Just the JSON object.';

  const text = await complete(prompt, systemWithFormat, {
    ...opts,
    temperature: opts.temperature ?? 0.3,
  });

  // Strip any accidental markdown code blocks
  const cleaned = text.replace(/```json?\n?/g, '').replace(/```\n?/g, '').trim();
  return JSON.parse(cleaned);
}

/**
 * Long-form generation with higher token limit
 */
async function generate(prompt, system = '', opts = {}) {
  return complete(prompt, system, {
    ...opts,
    maxTokens: opts.maxTokens || 8192,
    model: opts.model || 'claude-sonnet-4-20250514',
  });
}

module.exports = {
  getClient,
  complete,
  chat,
  json,
  generate,
};
