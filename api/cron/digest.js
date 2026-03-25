/**
 * Cron endpoint for daily digest
 * Triggered by Vercel Cron at 7 AM UTC (9 AM SAST)
 */
const { waitUntil } = require('@vercel/functions');
const digest = require('../../agents/daily-digest');

module.exports = async (req, res) => {
  // Pause toggle — set DIGEST_PAUSED=true in Vercel env to skip
  if (process.env.DIGEST_PAUSED === 'true') {
    return res.status(200).json({ ok: true, message: 'Digest paused' });
  }

  // Verify cron authorization
  const authHeader = req.headers.authorization;
  if (process.env.CRON_SECRET && authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const channel = req.query?.channel || undefined;
  waitUntil(digest.execute({ channel }));
  res.status(200).json({ ok: true, message: 'Digest triggered' });
};
