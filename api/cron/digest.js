/**
 * Cron endpoint for daily digest
 * Triggered by Vercel Cron at 7 AM UTC (9 AM SAST)
 */
const { waitUntil } = require('@vercel/functions');
const digest = require('../../agents/daily-digest');

module.exports = async (req, res) => {
  // Verify cron authorization
  const authHeader = req.headers.authorization;
  if (process.env.CRON_SECRET && authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  waitUntil(digest.execute());
  res.status(200).json({ ok: true, message: 'Digest triggered' });
};
