/**
 * QA Checker Agent
 * Reviews deliverables against quality standards
 * Ported from johnny-qa-bot
 */
const ai = require('../shared/ai');
const db = require('../shared/airtable');
const slack = require('../shared/slack');

const SYSTEM = `You are PROR's QA checker. You review link building placements and Reddit comments for quality.

For LINKS, check:
- Is the placement on a real, relevant site?
- Is the DR accurate?
- Is the anchor text natural and relevant?
- Is the link dofollow?
- Is the surrounding content quality (not spam/PBN)?

For REDDIT COMMENTS, check:
- Does it sound like a real person?
- Is it relevant to the thread context?
- Is brand mention natural, not forced?
- Does it provide genuine value?
- Is it unique from other comments in the batch?

Score each item: PASS, WARN, or FAIL with a brief reason.`;

async function execute(args) {
  const { client, channel, raw } = args;

  if (!client) {
    return slack.post(channel, '⚠️ Usage: `/qa [client] [type]` — type: links, reddit, all');
  }

  const type = (raw || '').includes('reddit') ? 'reddit' :
               (raw || '').includes('link') ? 'links' : 'all';

  await slack.post(channel, `🔍 Running QA for *${client}* (${type})...`);

  try {
    if (type === 'links' || type === 'all') {
      const links = await db.getLinks(client);
      if (links.length) {
        const result = await qaLinks(links, client);
        await slack.post(channel, result);
      } else {
        await slack.post(channel, `No links found for ${client} to QA.`);
      }
    }

    if (type === 'reddit' || type === 'all') {
      const reddit = await db.getReddit(client, { status: 'Drafted' });
      if (reddit.length) {
        const result = await qaReddit(reddit, client);
        await slack.post(channel, result);
      } else {
        await slack.post(channel, `No drafted Reddit comments found for ${client} to QA.`);
      }
    }
  } catch (err) {
    await slack.post(channel, `❌ QA Error: ${err.message}`);
  }
}

async function qaLinks(links, clientName) {
  const prompt = `QA these link placements for client "${clientName}":

${links.map((l, i) => `${i + 1}. URL: ${l.URL || 'N/A'} | DR: ${l.DR || 'N/A'} | Anchor: ${l['Anchor Text'] || 'N/A'} | Target: ${l['Target Page'] || 'N/A'}`).join('\n')}

For each link, respond with: PASS ✅, WARN ⚠️, or FAIL ❌ and a brief reason.
End with an overall summary.`;

  const result = await ai.complete(prompt, SYSTEM);
  return `📋 *Link QA — ${clientName}*\n\n${result}`;
}

async function qaReddit(comments, clientName) {
  const prompt = `QA these Reddit comments for client "${clientName}":

${comments.map((c, i) => `${i + 1}. [r/${c.Subreddit || 'unknown'}] ${c.Content?.substring(0, 200) || 'N/A'}...`).join('\n\n')}

For each comment, respond with: PASS ✅, WARN ⚠️, or FAIL ❌ and a brief reason.
Check for: authenticity, uniqueness, value, brand mention naturalness.
End with an overall summary and pass rate.`;

  const result = await ai.complete(prompt, SYSTEM);
  return `📋 *Reddit QA — ${clientName}*\n\n${result}`;
}

async function handleAction(action, data, context) {
  // Future: re-run QA, approve with notes
}

module.exports = { execute, handleAction };
