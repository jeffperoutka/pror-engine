/**
 * self-improve.js — Weekly self-improvement engine for PROR outreach
 *
 * Runs every Sunday night via Vercel cron. Continuously optimizes:
 *   1. Copy — A/B test subject lines & opening lines, auto-promote winners
 *   2. Negotiation — Adjust price ladder based on Jeff's feedback patterns
 *   3. Timing — Find best send windows per client vertical
 *   4. Domain/sender health correlation — Early warning on reply rate drops
 *   5. Weekly report — Post optimization summary to Slack #command-center
 *
 * Architecture:
 *   - Pulls campaign stats from Brevo API (last 7 days per client)
 *   - Pulls negotiation + A/B test data from Airtable
 *   - Uses z-test for proportions at 95% confidence
 *   - Generates new challenger copy via Claude Haiku
 *   - Posts optimization report to Slack
 */

const { waitUntil } = require('@vercel/functions');
const slack = require('../../shared/slack');
const discord = require('../../shared/discord');
const ai = require('../../shared/ai');
const airtable = require('../../shared/airtable');
const brevo = require('../../shared/brevo');
const abTests = require('../../shared/ab-tests');

const CHANNEL = () => process.env.CHANNEL_COMMAND_CENTER;

// ── Z-Test for Proportion Comparison ────────────────────────────────────────

/**
 * Two-proportion z-test for A/B test significance.
 * Returns z-score, whether result is significant at 95%, and the winner.
 */
function zTest(successA, nA, successB, nB) {
  if (nA === 0 || nB === 0) return { z: 0, significant: false, winner: null, pA: 0, pB: 0 };

  const pA = successA / nA;
  const pB = successB / nB;
  const pPool = (successA + successB) / (nA + nB);
  const se = Math.sqrt(pPool * (1 - pPool) * (1 / nA + 1 / nB));

  if (se === 0) return { z: 0, significant: false, winner: null, pA, pB };

  const z = (pA - pB) / se;
  const significant = Math.abs(z) > 1.96; // 95% confidence
  return { z, significant, winner: z > 0 ? 'A' : 'B', pA, pB };
}

// ── Brevo Campaign Stats ────────────────────────────────────────────────────

/**
 * Pull all sent campaigns from Brevo for the last N days.
 * Returns array of campaign objects with stats.
 */
async function getRecentCampaigns(days = 7) {
  const now = new Date();
  const startDate = new Date(now - days * 24 * 60 * 60 * 1000);
  const start = startDate.toISOString().split('T')[0];
  const end = now.toISOString().split('T')[0];

  const allCampaigns = [];
  let offset = 0;
  const limit = 50;
  let hasMore = true;

  while (hasMore) {
    try {
      const data = await brevo.brevoFetch(
        `/emailCampaigns?status=sent&limit=${limit}&offset=${offset}&startDate=${start}&endDate=${end}`
      );
      const campaigns = data.campaigns || [];
      allCampaigns.push(...campaigns);
      hasMore = campaigns.length === limit;
      offset += limit;
    } catch (err) {
      console.error('[self-improve] Brevo campaign fetch error:', err.message);
      hasMore = false;
    }
  }

  return allCampaigns;
}

/**
 * Aggregate campaign stats by client (extracted from campaign name).
 * Campaign names follow pattern: "CLIENTSLUG - T1 SEQ1 ..."
 */
function aggregateByClient(campaigns) {
  const byClient = {};

  for (const c of campaigns) {
    const name = c.name || '';
    const slug = name.split(' - ')[0]?.trim()?.toLowerCase() || 'unknown';
    const stats = c.statistics?.globalStats || c.statistics?.campaignStats?.[0] || {};

    if (!byClient[slug]) {
      byClient[slug] = {
        slug,
        campaigns: 0,
        sent: 0,
        delivered: 0,
        opens: 0,
        clicks: 0,
        replies: 0,
        bounces: 0,
        complaints: 0,
        byHour: {},
        byDow: {},
        subjects: [],
        senderDomains: {},
      };
    }

    const b = byClient[slug];
    b.campaigns++;
    b.sent += stats.sent || 0;
    b.delivered += stats.delivered || 0;
    b.opens += stats.uniqueOpens || 0;
    b.clicks += stats.uniqueClicks || 0;
    b.bounces += (stats.hardBounces || 0) + (stats.softBounces || 0);
    b.complaints += stats.complaints || stats.spamReports || 0;

    // Track subject lines and their performance
    if (c.subject) {
      b.subjects.push({
        subject: c.subject,
        sent: stats.sent || 0,
        opens: stats.uniqueOpens || 0,
        openRate: stats.sent > 0 ? (stats.uniqueOpens || 0) / stats.sent : 0,
      });
    }

    // Track timing data
    if (c.sentDate || c.scheduledAt) {
      const sentAt = new Date(c.sentDate || c.scheduledAt);
      const hour = sentAt.getUTCHours();
      const dow = sentAt.getUTCDay(); // 0=Sun, 1=Mon, ...

      if (!b.byHour[hour]) b.byHour[hour] = { sent: 0, opens: 0 };
      b.byHour[hour].sent += stats.sent || 0;
      b.byHour[hour].opens += stats.uniqueOpens || 0;

      if (!b.byDow[dow]) b.byDow[dow] = { sent: 0, opens: 0 };
      b.byDow[dow].sent += stats.sent || 0;
      b.byDow[dow].opens += stats.uniqueOpens || 0;
    }

    // Track sender domain performance
    const senderEmail = c.sender?.email || '';
    const senderDomain = senderEmail.split('@')[1] || 'unknown';
    if (!b.senderDomains[senderDomain]) {
      b.senderDomains[senderDomain] = { sent: 0, opens: 0, replies: 0 };
    }
    b.senderDomains[senderDomain].sent += stats.sent || 0;
    b.senderDomains[senderDomain].opens += stats.uniqueOpens || 0;
  }

  return byClient;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 1. COPY OPTIMIZATION
// ═══════════════════════════════════════════════════════════════════════════════

async function optimizeCopy(clientStats) {
  const results = {
    subjectTest: null,
    openingTest: null,
    newChallengers: [],
    errors: [],
  };

  try {
    // Get A/B test data from Airtable
    const allTests = await airtable.getABTests('running');

    // ── Subject line test ──
    const subjectRecords = allTests.filter(r => r.TestName === 'subject-personalization-v1');
    const subjectStats = { A: { sent: 0, opened: 0 }, B: { sent: 0, opened: 0 } };

    for (const r of subjectRecords) {
      const v = r.Variant;
      if (!subjectStats[v]) continue;
      if (r.Metric === 'sent') subjectStats[v].sent += r.Result || 1;
      if (r.Metric === 'opened') subjectStats[v].opened += r.Result || 1;
    }

    const subjectResult = zTest(
      subjectStats.A.opened, subjectStats.A.sent,
      subjectStats.B.opened, subjectStats.B.sent
    );

    const subjectTestDef = abTests.ACTIVE_TESTS['subject-personalization-v1'];
    results.subjectTest = {
      testName: 'subject-personalization-v1',
      metric: 'open_rate',
      variantA: subjectTestDef?.variants?.A?.label || 'Variant A',
      variantB: subjectTestDef?.variants?.B?.label || 'Variant B',
      rateA: (subjectResult.pA * 100).toFixed(1),
      rateB: (subjectResult.pB * 100).toFixed(1),
      nA: subjectStats.A.sent,
      nB: subjectStats.B.sent,
      totalSamples: subjectStats.A.sent + subjectStats.B.sent,
      significant: subjectResult.significant,
      winner: subjectResult.significant ? subjectResult.winner : null,
    };

    // If significant winner, generate new challenger
    if (subjectResult.significant) {
      const winnerLabel = subjectResult.winner === 'A' ? subjectTestDef?.variants?.A : subjectTestDef?.variants?.B;
      const winnerSubject = winnerLabel?.subjectTemplate || 'Content collaboration opportunity';

      try {
        const newSubject = await ai.complete(
          `The winning subject line template is: "${winnerSubject}"\n\n` +
          `Performance data:\n` +
          `- Variant A open rate: ${(subjectResult.pA * 100).toFixed(1)}% (n=${subjectStats.A.sent})\n` +
          `- Variant B open rate: ${(subjectResult.pB * 100).toFixed(1)}% (n=${subjectStats.B.sent})\n\n` +
          `Generate ONE new subject line variation to test against the winner. ` +
          `Return ONLY the subject line text, nothing else. Under 80 characters. ` +
          `Keep it professional and suitable for B2B outreach about content/link placements.`,
          'You are an email copywriting optimizer for B2B link-building outreach. Given the winning subject/body, generate one creative variation to test. Keep it professional, personalized, and under 80 chars for subjects.',
          { model: 'claude-haiku-4-5-20251001', maxTokens: 100, temperature: 0.8 }
        );

        results.subjectTest.newChallenger = newSubject.trim();
        results.newChallengers.push({
          testName: 'subject-personalization-v1',
          type: 'subject',
          content: newSubject.trim(),
        });

        // Log the completed test to Airtable
        await airtable.logABTest({
          testName: 'subject-personalization-v1',
          variant: subjectResult.winner,
          metric: 'open_rate',
          sampleSize: subjectStats.A.sent + subjectStats.B.sent,
          result: Math.max(subjectResult.pA, subjectResult.pB),
          status: 'completed',
        });
      } catch (err) {
        results.errors.push(`Subject challenger generation failed: ${err.message}`);
      }
    }

    // ── Opening line test ──
    const openingRecords = allTests.filter(r => r.TestName === 'opening-line-v1');
    const openingStats = { A: { sent: 0, replied: 0 }, B: { sent: 0, replied: 0 } };

    for (const r of openingRecords) {
      const v = r.Variant;
      if (!openingStats[v]) continue;
      if (r.Metric === 'sent') openingStats[v].sent += r.Result || 1;
      if (r.Metric === 'replied') openingStats[v].replied += r.Result || 1;
    }

    const openingResult = zTest(
      openingStats.A.replied, openingStats.A.sent,
      openingStats.B.replied, openingStats.B.sent
    );

    const openingTestDef = abTests.ACTIVE_TESTS['opening-line-v1'];
    results.openingTest = {
      testName: 'opening-line-v1',
      metric: 'reply_rate',
      variantA: openingTestDef?.variants?.A?.label || 'Variant A',
      variantB: openingTestDef?.variants?.B?.label || 'Variant B',
      rateA: (openingResult.pA * 100).toFixed(1),
      rateB: (openingResult.pB * 100).toFixed(1),
      nA: openingStats.A.sent,
      nB: openingStats.B.sent,
      totalSamples: openingStats.A.sent + openingStats.B.sent,
      significant: openingResult.significant,
      winner: openingResult.significant ? openingResult.winner : null,
    };

    if (openingResult.significant) {
      const winnerDef = openingResult.winner === 'A' ? openingTestDef?.variants?.A : openingTestDef?.variants?.B;
      const winnerBody = winnerDef?.bodyPrefix || '';

      try {
        const newOpening = await ai.complete(
          `The winning opening line is: "${winnerBody}"\n\n` +
          `Performance data:\n` +
          `- Variant A reply rate: ${(openingResult.pA * 100).toFixed(1)}% (n=${openingStats.A.sent})\n` +
          `- Variant B reply rate: ${(openingResult.pB * 100).toFixed(1)}% (n=${openingStats.B.sent})\n\n` +
          `Generate ONE new opening line variation to test against the winner. ` +
          `Return ONLY the opening line text, nothing else. 1-2 sentences max. ` +
          `It can reference {{params.ARTICLE_TITLE}} or {{params.SITE_NAME}} for personalization.`,
          'You are an email copywriting optimizer for B2B link-building outreach. Generate a creative opening line variation. Keep it professional, personalized, and conversational.',
          { model: 'claude-haiku-4-5-20251001', maxTokens: 150, temperature: 0.8 }
        );

        results.openingTest.newChallenger = newOpening.trim();
        results.newChallengers.push({
          testName: 'opening-line-v1',
          type: 'opening',
          content: newOpening.trim(),
        });

        await airtable.logABTest({
          testName: 'opening-line-v1',
          variant: openingResult.winner,
          metric: 'reply_rate',
          sampleSize: openingStats.A.sent + openingStats.B.sent,
          result: Math.max(openingResult.pA, openingResult.pB),
          status: 'completed',
        });
      } catch (err) {
        results.errors.push(`Opening challenger generation failed: ${err.message}`);
      }
    }
  } catch (err) {
    results.errors.push(`Copy optimization failed: ${err.message}`);
  }

  return results;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 2. NEGOTIATION OPTIMIZATION
// ═══════════════════════════════════════════════════════════════════════════════

async function optimizeNegotiations() {
  const results = {
    totalNegotiations: 0,
    winRate: 0,
    avgDiscount: 0,
    roundAvgs: {},
    feedbackDist: { good: 0, bad: 0, too_high: 0, too_low: 0 },
    priceLadderAdjustment: null,
    avgFinalVsInitial: 0,
    errors: [],
  };

  try {
    const base = airtable.getBase();
    const weekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();

    // Pull all negotiation records from last 7 days
    let negotiations = [];
    try {
      const records = await base('Negotiations').select({
        filterByFormula: `IS_AFTER({Date}, "${weekAgo}")`,
        sort: [{ field: 'Date', direction: 'asc' }],
      }).all();
      negotiations = records.map(r => ({ id: r.id, ...r.fields }));
    } catch (err) {
      // Table may not exist yet or be empty
      console.error('[self-improve] Negotiations fetch:', err.message);
      results.errors.push('Could not fetch negotiations — table may not exist yet');
      return results;
    }

    if (negotiations.length === 0) {
      results.errors.push('No negotiation data for last 7 days');
      return results;
    }

    results.totalNegotiations = negotiations.length;

    // ── Calculate round-by-round averages ──
    const roundData = {};
    const domains = new Set();
    let totalInitialAsk = 0;
    let totalFinalPrice = 0;
    let dealsWithBothPrices = 0;

    for (const n of negotiations) {
      const round = n.Round || 1;
      domains.add(n.Domain);

      if (!roundData[round]) roundData[round] = { offers: [], theirPrices: [] };
      if (n.OurOffer) roundData[round].offers.push(n.OurOffer);
      if (n.TheirPrice) roundData[round].theirPrices.push(n.TheirPrice);
    }

    // Calculate avg discount per round
    for (const [round, data] of Object.entries(roundData)) {
      const avgOffer = data.offers.length > 0
        ? data.offers.reduce((a, b) => a + b, 0) / data.offers.length
        : null;
      const avgTheirPrice = data.theirPrices.length > 0
        ? data.theirPrices.reduce((a, b) => a + b, 0) / data.theirPrices.length
        : null;

      results.roundAvgs[round] = {
        avgOffer: avgOffer ? Math.round(avgOffer) : null,
        avgTheirPrice: avgTheirPrice ? Math.round(avgTheirPrice) : null,
        discount: avgOffer && avgTheirPrice
          ? Math.round((1 - avgOffer / avgTheirPrice) * 100)
          : null,
        count: data.offers.length + data.theirPrices.length,
      };
    }

    // ── Win rate: domains with price_confirmed or status=accepted ──
    // Group negotiations by domain to find completed deals
    const byDomain = {};
    for (const n of negotiations) {
      if (!byDomain[n.Domain]) byDomain[n.Domain] = [];
      byDomain[n.Domain].push(n);
    }

    let dealsWon = 0;
    for (const [domain, domainNegs] of Object.entries(byDomain)) {
      const hasWin = domainNegs.some(n =>
        n.Action === 'accept' ||
        n.Sentiment === 'positive' && n.Action === 'negotiate' && domainNegs.length >= 2
      );
      if (hasWin) dealsWon++;

      // Track initial vs final price
      const firstPrice = domainNegs.find(n => n.TheirPrice)?.TheirPrice;
      const lastOffer = [...domainNegs].reverse().find(n => n.OurOffer)?.OurOffer;
      if (firstPrice && lastOffer) {
        totalInitialAsk += firstPrice;
        totalFinalPrice += lastOffer;
        dealsWithBothPrices++;
      }
    }

    const totalDomains = Object.keys(byDomain).length;
    results.winRate = totalDomains > 0 ? Math.round((dealsWon / totalDomains) * 100) : 0;
    results.avgFinalVsInitial = dealsWithBothPrices > 0
      ? Math.round((totalFinalPrice / totalInitialAsk) * 100)
      : null;
    results.avgDiscount = results.avgFinalVsInitial
      ? 100 - results.avgFinalVsInitial
      : 0;

    // ── Jeff's feedback distribution ──
    // Pull feedback from negotiation records (JeffFeedback field)
    for (const n of negotiations) {
      const feedback = (n.JeffFeedback || n.Feedback || '').toLowerCase();
      if (feedback.includes('good') || feedback.includes('thumbsup')) results.feedbackDist.good++;
      else if (feedback.includes('bad') || feedback.includes('thumbsdown')) results.feedbackDist.bad++;
      else if (feedback.includes('too_high') || feedback.includes('toohigh')) results.feedbackDist.too_high++;
      else if (feedback.includes('too_low') || feedback.includes('toolow')) results.feedbackDist.too_low++;
    }

    const totalFeedback = results.feedbackDist.good + results.feedbackDist.bad +
      results.feedbackDist.too_high + results.feedbackDist.too_low;

    // ── Price ladder adjustment ──
    if (totalFeedback > 0) {
      const tooHighPct = results.feedbackDist.too_high / totalFeedback;
      const tooLowPct = results.feedbackDist.too_low / totalFeedback;

      if (tooHighPct > 0.30) {
        results.priceLadderAdjustment = {
          direction: 'lower',
          reason: `${Math.round(tooHighPct * 100)}% of feedback says "too high"`,
          change: 'Reducing offer percentages by 5% (e.g., Round 1: 50% -> 45%)',
        };

        // Store adjusted ladder in Airtable config
        try {
          await base('Config').create([{
            fields: {
              Key: 'price_ladder_adjustment',
              Value: JSON.stringify({
                date: new Date().toISOString(),
                direction: 'lower',
                adjustment: -0.05,
                reason: results.priceLadderAdjustment.reason,
              }),
              UpdatedAt: new Date().toISOString(),
            },
          }]);
        } catch (err) {
          results.errors.push(`Config update failed: ${err.message}`);
        }
      } else if (tooLowPct > 0.30) {
        results.priceLadderAdjustment = {
          direction: 'higher',
          reason: `${Math.round(tooLowPct * 100)}% of feedback says "too low"`,
          change: 'Raising offer percentages by 5% (e.g., Round 1: 50% -> 55%)',
        };

        try {
          await base('Config').create([{
            fields: {
              Key: 'price_ladder_adjustment',
              Value: JSON.stringify({
                date: new Date().toISOString(),
                direction: 'higher',
                adjustment: 0.05,
                reason: results.priceLadderAdjustment.reason,
              }),
              UpdatedAt: new Date().toISOString(),
            },
          }]);
        } catch (err) {
          results.errors.push(`Config update failed: ${err.message}`);
        }
      }
    }

    // ── Flag low win rate ──
    if (results.winRate < 20 && totalDomains >= 10) {
      results.lowWinRateAlert = {
        winRate: results.winRate,
        totalDomains,
        suggestion: 'Win rate below 20%. Consider more aggressive follow-ups or adjusting price anchors.',
      };
    }
  } catch (err) {
    results.errors.push(`Negotiation optimization failed: ${err.message}`);
  }

  return results;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 3. TIMING OPTIMIZATION
// ═══════════════════════════════════════════════════════════════════════════════

async function optimizeTiming(clientStats) {
  const results = {
    bestHours: [],
    worstHours: [],
    bestDays: [],
    worstDays: [],
    followUpTestResult: null,
    errors: [],
  };

  try {
    // Aggregate timing data across all clients
    const hourlyStats = {};
    const dailyStats = {};
    const dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

    for (const client of Object.values(clientStats)) {
      for (const [hour, data] of Object.entries(client.byHour)) {
        if (!hourlyStats[hour]) hourlyStats[hour] = { sent: 0, opens: 0 };
        hourlyStats[hour].sent += data.sent;
        hourlyStats[hour].opens += data.opens;
      }
      for (const [dow, data] of Object.entries(client.byDow)) {
        if (!dailyStats[dow]) dailyStats[dow] = { sent: 0, opens: 0 };
        dailyStats[dow].sent += data.sent;
        dailyStats[dow].opens += data.opens;
      }
    }

    // Find best/worst hours (only hours with meaningful data)
    const hourEntries = Object.entries(hourlyStats)
      .filter(([_, d]) => d.sent >= 20)
      .map(([hour, d]) => ({
        hour: parseInt(hour),
        openRate: d.opens / d.sent,
        sent: d.sent,
      }))
      .sort((a, b) => b.openRate - a.openRate);

    if (hourEntries.length >= 2) {
      results.bestHours = hourEntries.slice(0, 3).map(h => ({
        hour: `${h.hour}:00 UTC`,
        openRate: (h.openRate * 100).toFixed(1) + '%',
        sent: h.sent,
      }));
      results.worstHours = hourEntries.slice(-2).map(h => ({
        hour: `${h.hour}:00 UTC`,
        openRate: (h.openRate * 100).toFixed(1) + '%',
        sent: h.sent,
      }));
    }

    // Find best/worst days
    const dayEntries = Object.entries(dailyStats)
      .filter(([_, d]) => d.sent >= 20)
      .map(([dow, d]) => ({
        day: dayNames[parseInt(dow)],
        openRate: d.opens / d.sent,
        sent: d.sent,
      }))
      .sort((a, b) => b.openRate - a.openRate);

    if (dayEntries.length >= 2) {
      results.bestDays = dayEntries.slice(0, 3);
      results.worstDays = dayEntries.slice(-2);
    }

    // ── Follow-up timing A/B test ──
    try {
      const allTests = await airtable.getABTests('running');
      const timingRecords = allTests.filter(r => r.TestName === 'followup-timing-v1');
      const timingStats = { A: { sent: 0, replied: 0 }, B: { sent: 0, replied: 0 } };

      for (const r of timingRecords) {
        const v = r.Variant;
        if (!timingStats[v]) continue;
        if (r.Metric === 'sent') timingStats[v].sent += r.Result || 1;
        if (r.Metric === 'replied') timingStats[v].replied += r.Result || 1;
      }

      const timingResult = zTest(
        timingStats.A.replied, timingStats.A.sent,
        timingStats.B.replied, timingStats.B.sent
      );

      const timingTestDef = abTests.ACTIVE_TESTS['followup-timing-v1'];
      results.followUpTestResult = {
        variantA: timingTestDef?.variants?.A?.label || 'Standard timing',
        variantB: timingTestDef?.variants?.B?.label || 'Faster timing',
        rateA: (timingResult.pA * 100).toFixed(1),
        rateB: (timingResult.pB * 100).toFixed(1),
        nA: timingStats.A.sent,
        nB: timingStats.B.sent,
        significant: timingResult.significant,
        winner: timingResult.significant ? timingResult.winner : null,
        // Check for sustained >10% difference (even if not yet significant)
        relativeDiff: timingResult.pA > 0
          ? Math.abs((timingResult.pB - timingResult.pA) / timingResult.pA * 100).toFixed(1)
          : '0.0',
      };
    } catch (err) {
      results.errors.push(`Follow-up timing test fetch failed: ${err.message}`);
    }
  } catch (err) {
    results.errors.push(`Timing optimization failed: ${err.message}`);
  }

  return results;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 4. DOMAIN/SENDER HEALTH CORRELATION
// ═══════════════════════════════════════════════════════════════════════════════

async function analyzeSenderHealth(clientStats) {
  const results = {
    flagged: [],
    healthy: 0,
    total: 0,
    errors: [],
  };

  try {
    const base = airtable.getBase();

    // Pull previous week's sender health data for comparison
    const twoWeeksAgo = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    const oneWeekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

    let prevWeekHealth = [];
    try {
      const records = await base('SenderHealth').select({
        filterByFormula: `AND(IS_AFTER({Date}, "${twoWeeksAgo}"), IS_BEFORE({Date}, "${oneWeekAgo}"))`,
      }).all();
      prevWeekHealth = records.map(r => ({ id: r.id, ...r.fields }));
    } catch {
      // SenderHealth table may not have historical data yet
    }

    // Build prev week lookup by domain
    const prevByDomain = {};
    for (const h of prevWeekHealth) {
      const domain = h.Domain;
      if (!prevByDomain[domain] || new Date(h.Date) > new Date(prevByDomain[domain].Date)) {
        prevByDomain[domain] = h;
      }
    }

    // Analyze current sender domains from campaign data
    const allSenderDomains = new Set();
    for (const client of Object.values(clientStats)) {
      for (const domain of Object.keys(client.senderDomains)) {
        allSenderDomains.add(domain);
      }
    }

    results.total = allSenderDomains.size;

    for (const domain of allSenderDomains) {
      // Aggregate current week stats for this sender domain
      let currentSent = 0;
      let currentOpens = 0;
      let currentReplies = 0;

      for (const client of Object.values(clientStats)) {
        const sd = client.senderDomains[domain];
        if (sd) {
          currentSent += sd.sent;
          currentOpens += sd.opens;
          currentReplies += sd.replies || 0;
        }
      }

      if (currentSent === 0) continue;

      const currentOpenRate = currentOpens / currentSent;
      const currentReplyRate = currentReplies / currentSent;

      // Compare with previous week
      const prev = prevByDomain[domain];
      if (prev && prev.OpenRate > 0) {
        const prevOpenRate = prev.OpenRate / 100; // Stored as percentage
        const replyDropPct = prev.ReplyRate > 0
          ? ((currentReplyRate - prev.ReplyRate / 100) / (prev.ReplyRate / 100)) * 100
          : 0;
        const openDropPct = prevOpenRate > 0
          ? ((currentOpenRate - prevOpenRate) / prevOpenRate) * 100
          : 0;

        // Reply rate drop >30% is a leading indicator — flag BEFORE open rates crater
        if (replyDropPct < -30) {
          results.flagged.push({
            domain,
            issue: 'reply_rate_drop',
            currentReplyRate: (currentReplyRate * 100).toFixed(2) + '%',
            prevReplyRate: (prev.ReplyRate || 0).toFixed(2) + '%',
            dropPct: Math.abs(replyDropPct).toFixed(0) + '%',
            currentOpenRate: (currentOpenRate * 100).toFixed(1) + '%',
            warning: 'Reply rate dropped >30% WoW — potential spam issue before open rates reflect it',
          });
        } else if (openDropPct < -30) {
          results.flagged.push({
            domain,
            issue: 'open_rate_drop',
            currentOpenRate: (currentOpenRate * 100).toFixed(1) + '%',
            prevOpenRate: (prevOpenRate * 100).toFixed(1) + '%',
            dropPct: Math.abs(openDropPct).toFixed(0) + '%',
            warning: 'Open rate dropped >30% WoW — sender reputation may be degraded',
          });
        } else {
          results.healthy++;
        }
      } else {
        results.healthy++; // No historical data — assume healthy
      }
    }
  } catch (err) {
    results.errors.push(`Sender health analysis failed: ${err.message}`);
  }

  return results;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 5. WEEKLY OPTIMIZATION REPORT
// ═══════════════════════════════════════════════════════════════════════════════

function buildWeeklyReport(copyResults, negResults, timingResults, senderResults, clientStats, prevWeekStats) {
  const now = new Date();
  const weekOf = now.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });

  // ── Aggregate totals ──
  let totalSent = 0, totalOpens = 0, totalReplies = 0;
  for (const c of Object.values(clientStats)) {
    totalSent += c.sent;
    totalOpens += c.opens;
  }
  const overallOpenRate = totalSent > 0 ? (totalOpens / totalSent * 100).toFixed(1) : '0.0';

  // ── Copy section ──
  let copySection = ':email: *COPY OPTIMIZATION*\n';

  if (copyResults.subjectTest) {
    const st = copyResults.subjectTest;
    if (st.significant && st.winner) {
      const winLabel = st.winner === 'A' ? st.variantA : st.variantB;
      const winRate = st.winner === 'A' ? st.rateA : st.rateB;
      const loseRate = st.winner === 'A' ? st.rateB : st.rateA;
      copySection += `• Subject test "${st.testName}": ${winLabel} winning (${winRate}% open vs ${loseRate}%)\n`;
      if (st.newChallenger) {
        copySection += `  → Auto-promoted. New challenger: "${st.newChallenger}"\n`;
      }
    } else {
      copySection += `• Subject test: No significant winner yet (n=${st.totalSamples}`;
      const minSample = abTests.ACTIVE_TESTS['subject-personalization-v1']?.minSampleSize || 50;
      const perVariant = Math.min(st.nA, st.nB);
      if (perVariant < minSample) {
        copySection += `, need n=${minSample} per variant`;
      }
      copySection += ')\n';
    }
  } else {
    copySection += '• Subject test: No data available\n';
  }

  if (copyResults.openingTest) {
    const ot = copyResults.openingTest;
    if (ot.significant && ot.winner) {
      const winLabel = ot.winner === 'A' ? ot.variantA : ot.variantB;
      const winRate = ot.winner === 'A' ? ot.rateA : ot.rateB;
      const loseRate = ot.winner === 'A' ? ot.rateB : ot.rateA;
      copySection += `• Opening line test: ${winLabel} winning (${winRate}% reply vs ${loseRate}%)\n`;
      if (ot.newChallenger) {
        copySection += `  → Auto-promoted. New challenger: "${ot.newChallenger}"\n`;
      }
    } else {
      copySection += `• Opening line test: No significant winner yet (n=${ot.totalSamples})\n`;
    }
  } else {
    copySection += '• Opening line test: No data available\n';
  }

  // ── Negotiation section ──
  let negSection = ':moneybag: *NEGOTIATION OPTIMIZATION*\n';

  if (negResults.totalNegotiations > 0) {
    negSection += `• Avg discount achieved: ${negResults.avgDiscount}% off asking price\n`;
    negSection += `• Win rate: ${negResults.winRate}%`;
    if (prevWeekStats?.winRate !== undefined) {
      const diff = negResults.winRate - prevWeekStats.winRate;
      negSection += ` (${diff >= 0 ? 'up' : 'down'} from ${prevWeekStats.winRate}% last week ${diff >= 0 ? '↑' : '↓'})`;
    }
    negSection += '\n';

    if (negResults.priceLadderAdjustment) {
      negSection += `• Price ladder: ${negResults.priceLadderAdjustment.change}\n`;
    } else {
      negSection += '• Price ladder adjustment: None needed (feedback balanced)\n';
    }

    // Feedback distribution
    const fd = negResults.feedbackDist;
    const totalFb = fd.good + fd.bad + fd.too_high + fd.too_low;
    if (totalFb > 0) {
      const goodPct = Math.round(fd.good / totalFb * 100);
      const badPct = Math.round(fd.bad / totalFb * 100);
      const highPct = Math.round(fd.too_high / totalFb * 100);
      const lowPct = Math.round(fd.too_low / totalFb * 100);
      negSection += `• Jeff feedback: :thumbsup: ${goodPct}% good, :thumbsdown: ${badPct}% bad, :money_with_wings: ${highPct}% too high, :chart_with_downwards_trend: ${lowPct}% too low\n`;
    }

    if (negResults.lowWinRateAlert) {
      negSection += `• :warning: ${negResults.lowWinRateAlert.suggestion}\n`;
    }
  } else {
    negSection += '• No negotiation data this week\n';
  }

  // ── Timing section ──
  let timingSection = ':alarm_clock: *TIMING INSIGHTS*\n';

  if (timingResults.bestDays.length > 0) {
    const bestDayStr = timingResults.bestDays.map(d => d.day).join('-');
    const bestHourStr = timingResults.bestHours.length > 0
      ? timingResults.bestHours.map(h => h.hour.replace(' UTC', '')).join(', ') + ' UTC'
      : 'insufficient data';
    timingSection += `• Best open window: ${bestDayStr} ${bestHourStr}\n`;
  } else {
    timingSection += '• Best open window: Insufficient data\n';
  }

  if (timingResults.worstDays.length > 0) {
    const worstStr = timingResults.worstDays.map(d => `${d.day} (${(d.openRate * 100).toFixed(1)}%)`).join(', ');
    timingSection += `• Worst: ${worstStr}\n`;
  }

  if (timingResults.followUpTestResult) {
    const ft = timingResults.followUpTestResult;
    if (ft.significant && ft.winner) {
      const winLabel = ft.winner === 'A' ? ft.variantA : ft.variantB;
      timingSection += `• Follow-up timing test: ${winLabel} winning (${ft.rateA}% vs ${ft.rateB}%)\n`;
    } else {
      timingSection += `• Follow-up timing test: ${ft.relativeDiff}% difference, not yet significant (n=${ft.nA + ft.nB})\n`;
    }
  }

  // ── Sender health section ──
  let senderSection = '';
  if (senderResults.flagged.length > 0) {
    senderSection = ':rotating_light: *SENDER HEALTH ALERTS*\n';
    for (const f of senderResults.flagged) {
      if (f.issue === 'reply_rate_drop') {
        senderSection += `• :warning: *${f.domain}*: Reply rate dropped ${f.dropPct} WoW (${f.prevReplyRate} → ${f.currentReplyRate}) — leading indicator, check before open rates follow\n`;
      } else {
        senderSection += `• :red_circle: *${f.domain}*: Open rate dropped ${f.dropPct} WoW (${f.prevOpenRate} → ${f.currentOpenRate})\n`;
      }
    }
  }

  // ── Trends section ──
  let trendsSection = ':bar_chart: *TRENDS (vs last week)*\n';
  trendsSection += `• Open rate: ${overallOpenRate}%`;
  if (prevWeekStats?.openRate !== undefined) {
    const diff = parseFloat(overallOpenRate) - prevWeekStats.openRate;
    const diffPct = prevWeekStats.openRate > 0
      ? ((diff / prevWeekStats.openRate) * 100).toFixed(1)
      : '0.0';
    trendsSection += ` (${diff >= 0 ? '↑' : '↓'} ${Math.abs(diffPct)}%)`;
  }
  trendsSection += '\n';

  trendsSection += `• Total sent: ${totalSent.toLocaleString()}\n`;
  trendsSection += `• Clients active: ${Object.keys(clientStats).length}\n`;

  // ── Assemble full report ──
  const sections = [
    `:brain: *Self-Improvement Report — Week of ${weekOf}*\n`,
    copySection,
    negSection,
    timingSection,
  ];

  if (senderSection) sections.push(senderSection);
  sections.push(trendsSection);

  // Add any errors as footnote
  const allErrors = [
    ...copyResults.errors,
    ...negResults.errors,
    ...timingResults.errors,
    ...senderResults.errors,
  ];
  if (allErrors.length > 0) {
    sections.push(`\n_:information_source: ${allErrors.length} non-fatal error(s) during analysis — check logs_`);
  }

  return sections.join('\n');
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN ORCHESTRATOR
// ═══════════════════════════════════════════════════════════════════════════════

async function run() {
  console.log('[self-improve] Starting weekly self-improvement analysis...');
  const startTime = Date.now();

  // ── Step 1: Pull campaign data from Brevo ──
  const campaigns = await getRecentCampaigns(7);
  console.log(`[self-improve] Fetched ${campaigns.length} campaigns from last 7 days`);

  const clientStats = aggregateByClient(campaigns);
  console.log(`[self-improve] Aggregated stats for ${Object.keys(clientStats).length} clients`);

  // ── Step 2: Pull previous week's stats for trend comparison ──
  let prevWeekStats = null;
  try {
    const prevCampaigns = await getRecentCampaigns(14);
    // Filter to only 7-14 days ago
    const oneWeekAgo = Date.now() - 7 * 24 * 60 * 60 * 1000;
    const twoWeeksAgo = Date.now() - 14 * 24 * 60 * 60 * 1000;
    const prevOnly = prevCampaigns.filter(c => {
      const sentDate = new Date(c.sentDate || c.scheduledAt || 0).getTime();
      return sentDate >= twoWeeksAgo && sentDate < oneWeekAgo;
    });
    const prevClientStats = aggregateByClient(prevOnly);

    let prevSent = 0, prevOpens = 0;
    for (const c of Object.values(prevClientStats)) {
      prevSent += c.sent;
      prevOpens += c.opens;
    }
    prevWeekStats = {
      openRate: prevSent > 0 ? parseFloat((prevOpens / prevSent * 100).toFixed(1)) : 0,
      sent: prevSent,
    };
  } catch (err) {
    console.error('[self-improve] Could not fetch previous week stats:', err.message);
  }

  // ── Step 3: Run all optimizations in parallel ──
  const [copyResults, negResults, timingResults, senderResults] = await Promise.all([
    optimizeCopy(clientStats),
    optimizeNegotiations(),
    optimizeTiming(clientStats),
    analyzeSenderHealth(clientStats),
  ]);

  console.log('[self-improve] All optimizations complete');

  // ── Step 4: Build and post report ──
  const report = buildWeeklyReport(
    copyResults, negResults, timingResults, senderResults,
    clientStats, prevWeekStats
  );

  const channel = CHANNEL();
  if (channel) {
    await slack.post(channel, report);
  }
  await discord.postIfConfigured('system-ops', report);
  console.log('[self-improve] Report posted');

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`[self-improve] Done in ${elapsed}s`);

  return {
    ok: true,
    elapsed: `${elapsed}s`,
    campaigns: campaigns.length,
    clients: Object.keys(clientStats).length,
    copyTests: {
      subject: copyResults.subjectTest?.significant ? 'winner_found' : 'running',
      opening: copyResults.openingTest?.significant ? 'winner_found' : 'running',
      newChallengers: copyResults.newChallengers.length,
    },
    negotiations: {
      total: negResults.totalNegotiations,
      winRate: negResults.winRate,
      ladderAdjusted: !!negResults.priceLadderAdjustment,
    },
    senderAlerts: senderResults.flagged.length,
    errors: [
      ...copyResults.errors,
      ...negResults.errors,
      ...timingResults.errors,
      ...senderResults.errors,
    ],
  };
}

// ── Vercel Serverless Handler ─────────────────────────────────────────────────

module.exports = async (req, res) => {
  // Auth check (same pattern as other cron endpoints)
  const authHeader = req.headers.authorization;
  if (process.env.CRON_SECRET && authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  if (req.method !== 'GET' && req.method !== 'POST') {
    return res.status(405).end();
  }

  // Run in background so we return fast
  const processingPromise = run()
    .then(async result => {
      console.log(`[self-improve] Complete:`, JSON.stringify(result));
      await airtable.logCronRun('self-improve').catch(e => console.error('[self-improve] logCronRun:', e.message));
    })
    .catch(err => {
      console.error('[self-improve] Fatal:', err);
    });

  waitUntil(processingPromise);

  res.status(200).json({ ok: true, message: 'Self-improvement engine triggered' });
};
