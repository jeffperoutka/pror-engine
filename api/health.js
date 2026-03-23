/**
 * Health check endpoint
 */
module.exports = async (req, res) => {
  res.json({
    status: 'ok',
    engine: 'pror-engine',
    version: '1.0.0',
    timestamp: new Date().toISOString(),
    agents: ['link-builder', 'reddit-strategist', 'qa-checker', 'orchestrator'],
  });
};
