/**
 * prospect-replenish.js — Weekly cron to auto-replenish prospects for all 13 PROR clients
 *
 * Pipeline: Check remaining → SERP scrape (DataForSEO) → Email lookup (AnyMailFinder) → Dedupe → Push to Brevo
 *
 * Vercel cron: runs weekly (Sunday evening) with 300s max duration
 * Processes clients in priority order (lowest remaining first), stops at 240s safety margin
 */
const { waitUntil } = require('@vercel/functions');
const slack = require('../../shared/slack');

// ─── Environment ───────────────────────────────────────────────────────────────
const BREVO_KEY = process.env.BREVO_API_KEY;
const DATAFORSEO_LOGIN = process.env.DATAFORSEO_LOGIN;
const DATAFORSEO_PASSWORD = process.env.DATAFORSEO_PASSWORD;
const ANYMAILFINDER_KEY = process.env.ANYMAILFINDER_KEY;
const AIRTABLE_PAT = process.env.AIRTABLE_PAT;
const AIRTABLE_BASE = process.env.AIRTABLE_BASE;
const CHANNEL = () => process.env.CHANNEL_COMMAND_CENTER;

const TIMEOUT_SAFETY_MS = 240_000; // Stop processing new clients after 240s
const REPLENISH_THRESHOLD = 150;   // Trigger replenish if < 150 unsent prospects remain
const SUFFIXES = ['write for us', 'guest post', 'submit article', 'contribute', 'guest author'];

// ─── Client Configs ────────────────────────────────────────────────────────────
const CLIENT_CONFIGS = [
  {
    slug: 'ABLE-AMMO',
    name: 'Able Ammo',
    keywords: ['ammunition store', 'gun accessories', 'hunting gear', 'shooting sports', 'firearm maintenance', 'ammo reviews', 'tactical gear', 'gun range', 'hunting season', 'reloading supplies'],
    senderWarmed: { id: 3, email: 'yam@ecom-ranker.com', name: 'Yam Roar' },
    senderNew: { id: 28, email: 'yam@ableammo.ecom-ranker.com', name: 'Yam Roar' },
  },
  {
    slug: 'DR-DABBER',
    name: 'Dr. Dabber',
    keywords: ['vaporizer review', 'concentrate pen', 'dab pen guide', 'cannabis vaporizer', 'wax pen', 'electric dab rig', 'vape technology', 'dabbing guide', 'portable vaporizer', 'concentrate guide'],
    senderWarmed: { id: 4, email: 'yam@ecom-guestposts.com', name: 'Yam Roar' },
    senderNew: { id: 29, email: 'yam@drdabber.ecom-guestposts.com', name: 'Yam Roar' },
  },
  {
    slug: 'FELINA',
    name: 'Felina',
    keywords: ['lingerie guide', 'bra fitting tips', 'loungewear trends', 'intimates fashion', 'comfortable bra', 'women sleepwear', 'body positive fashion', 'underwear guide', 'shapewear review', 'sustainable intimates'],
    senderWarmed: { id: 5, email: 'yam@guestpost-now.com', name: 'Yam Roar' },
    senderNew: { id: 30, email: 'yam@felina.guest-poster.com', name: 'Yam Roar' },
  },
  {
    slug: 'MILL-PACKAGING',
    name: 'Mill Packaging',
    keywords: ['custom packaging', 'sustainable packaging', 'product packaging design', 'unboxing experience', 'packaging materials', 'ecommerce packaging', 'branded packaging', 'packaging trends', 'shipping boxes', 'eco packaging'],
    senderWarmed: { id: 6, email: 'yam@linkinsertion.live', name: 'Yam Roar' },
    senderNew: { id: 31, email: 'yam@millpkg.linkinsertion.live', name: 'Yam Roar' },
  },
  {
    slug: 'PRIMERX',
    name: 'PrimeRx',
    keywords: ['pharmacy software', 'pharmacy management', 'pharmacy automation', 'prescription management', 'pharmacy technology', 'pharmacy POS', 'medication management', 'pharmacy inventory', 'healthcare technology', 'pharmacy workflow'],
    senderWarmed: { id: 7, email: 'yam@guest-poster.com', name: 'Yam Roar' },
    senderNew: { id: 32, email: 'yam@primerx.guestpost-now.com', name: 'Yam Roar' },
  },
  {
    slug: 'SMOKEA',
    name: 'SMOKEA',
    keywords: ['smoke shop', 'glass pipes', 'smoking accessories', 'bong guide', 'rolling papers', 'vape shop', 'grinder review', 'water pipe', 'dab tools', 'smoke shop trends'],
    senderWarmed: { id: 8, email: 'yam@guest-post.live', name: 'Yam Roar' },
    senderNew: { id: 33, email: 'yam@smokea.guest-post.live', name: 'Yam Roar' },
  },
  {
    slug: 'MRSKIN',
    name: 'MrSkin',
    keywords: ['movie scenes database', 'celebrity filmography', 'movie reviews', 'TV show guide', 'entertainment news', 'film database', 'streaming reviews', 'movie recommendations', 'celebrity news', 'film industry'],
    senderWarmed: { id: 9, email: 'yam@ecom-links.com', name: 'Yam Roar' },
    senderNew: { id: 34, email: 'yam@mrskin.ecom-links.com', name: 'Yam Roar' },
  },
  {
    slug: 'VRAI',
    name: 'VRAI',
    keywords: ['lab grown diamonds', 'sustainable jewelry', 'engagement ring guide', 'diamond education', 'ethical jewelry', 'fine jewelry trends', 'wedding ring guide', 'jewelry care tips', 'luxury jewelry', 'diamond buying guide'],
    senderWarmed: { id: 10, email: 'yam@linkinsertion.us', name: 'Yam Roar' },
    senderNew: { id: 35, email: 'yam@vrai.linkinsertion.us', name: 'Yam Roar' },
  },
  {
    slug: 'AMS-FULFILLMENT',
    name: 'AMS Fulfillment',
    keywords: ['3PL services', 'order fulfillment', 'ecommerce fulfillment', 'warehouse management', 'shipping solutions', 'fulfillment center', 'inventory management', 'pick and pack', 'logistics provider', 'supply chain management'],
    senderWarmed: { id: 11, email: 'yam@kobo.linkinsertion.org', name: 'Yam Roar' },
    senderNew: { id: 36, email: 'yam@amsfulfillment.ecom-ranker.com', name: 'Yam Roar' },
  },
  {
    slug: 'BUILT-BAR',
    name: 'Built Bar',
    keywords: ['protein bar review', 'healthy snack', 'fitness nutrition', 'protein snack', 'meal replacement bar', 'workout nutrition', 'low sugar snack', 'protein bar comparison', 'gym snacks', 'on the go nutrition'],
    senderWarmed: { id: 12, email: 'yam@opensea.linkinsertion.org', name: 'Yam Roar' },
    senderNew: { id: 37, email: 'yam@builtbar.ecom-guestposts.com', name: 'Yam Roar' },
  },
  {
    slug: 'NUTRABIO',
    name: 'NutraBio',
    keywords: ['sports nutrition', 'supplement review', 'pre workout guide', 'protein powder review', 'supplement transparency', 'bodybuilding nutrition', 'amino acids guide', 'creatine review', 'workout supplements', 'muscle building nutrition'],
    senderWarmed: { id: 2, email: 'yam@linkinsertion.org', name: 'Yam Roar' },
    senderNew: { id: 38, email: 'yam@nutrabio.guest-poster.com', name: 'Yam Roar' },
  },
  {
    slug: 'VIVANTE-LIVING',
    name: 'Vivante Living',
    keywords: ['luxury furniture', 'home decor trends', 'interior design tips', 'living room design', 'modern furniture', 'home styling', 'luxury home goods', 'furniture buying guide', 'statement furniture', 'contemporary design'],
    senderWarmed: { id: 3, email: 'yam@ecom-ranker.com', name: 'Yam Roar' },
    senderNew: { id: 39, email: 'yam@vivante.linkinsertion.live', name: 'Yam Roar' },
  },
  {
    slug: 'GOODR',
    name: 'Goodr',
    keywords: ['running sunglasses', 'sports sunglasses', 'running gear', 'athletic eyewear', 'trail running', 'marathon gear', 'polarized sunglasses', 'cycling sunglasses', 'outdoor sports gear', 'runner accessories'],
    senderWarmed: { id: 4, email: 'yam@ecom-guestposts.com', name: 'Yam Roar' },
    senderNew: { id: 40, email: 'yam@goodr.guestpost-now.com', name: 'Yam Roar' },
  },
];

// ─── Email Templates ───────────────────────────────────────────────────────────
const TEMPLATES = {
  'ABLE-AMMO': { subjects: ['Quick idea for {{SITE_NAME}}','Re: Quick idea for {{SITE_NAME}}','Re: Quick idea for {{SITE_NAME}}','Re: Quick idea for {{SITE_NAME}}'], bodies: [`Hi {{FIRSTNAME}},\n\nI came across your piece on {{ARTICLE_TITLE}} — solid breakdown. I'm working with Able Ammo and we've been putting together content around ammunition selection, range safety, and hunting season prep that I think would fit well on your site.\n\nWould you be open to us contributing an article? We'd handle all the writing.\n\nBest,\nYam`,`Hey {{FIRSTNAME}}, just floating this back up. We have a few article ideas ready to go — happy to send topics over if that's easier.\n\nYam`,`Hi {{FIRSTNAME}}, wanted to check in one more time. We're flexible on topics and can match your editorial style. We're also happy to cover any editorial fees on your end. Let me know if there's interest.\n\nYam`,`Last one from me — if the timing isn't right, no worries at all. If content collaborations are something you do down the road, feel free to reach out.\n\nYam`] },
  'DR-DABBER': { subjects: ['Content collab with {{SITE_NAME}}?','Re: Content collab with {{SITE_NAME}}?','Re: Content collab with {{SITE_NAME}}?','Re: Content collab with {{SITE_NAME}}?'], bodies: [`Hi {{FIRSTNAME}},\n\nBeen following your content — your {{ARTICLE_TITLE}} was a great read. I work with Dr. Dabber (premium vaporizer brand) and we've got some article ideas around vaporizer tech, concentrate guides, and device comparisons that could work well for your audience.\n\nWould you be open to a contributed piece? We handle the writing.\n\nCheers,\nYam`,`Hey {{FIRSTNAME}}, just circling back. Happy to send over a few topic ideas tailored to your site if that helps.\n\nYam`,`Hi {{FIRSTNAME}}, checking in one last time on this. We can adapt to your guidelines and are glad to cover any editorial or publishing fees. Let me know if there's an opening.\n\nYam`,`Closing the loop — totally understand if it's not the right fit. Door's always open if you're looking for cannabis/vape content contributors down the road.\n\nYam`] },
  'FELINA': { subjects: ['Article idea for {{SITE_NAME}}','Re: Article idea for {{SITE_NAME}}','Re: Article idea for {{SITE_NAME}}','Re: Article idea for {{SITE_NAME}}'], bodies: [`Hi {{FIRSTNAME}},\n\nLove what you're doing with {{SITE_NAME}} — your post on {{ARTICLE_TITLE}} really resonated. I'm working with Felina (women's intimates & loungewear brand) and we'd love to contribute an article on topics like finding the perfect bra fit, loungewear essentials, or body-positive fashion.\n\nWould you be interested in a contributed piece? We write it, you publish it.\n\nBest,\nYam`,`Hey {{FIRSTNAME}}, just bumping this up. We have a few topics ready — happy to share them so you can pick what fits.\n\nYam`,`Hi {{FIRSTNAME}}, one more nudge. We write to your style guidelines and are happy to cover any editorial fees involved. Would love to make this work.\n\nYam`,`Last message from me — if the timing isn't right, no hard feelings. Happy to reconnect whenever it makes sense.\n\nYam`] },
  'MILL-PACKAGING': { subjects: ['Guest post for {{SITE_NAME}}?','Re: Guest post for {{SITE_NAME}}?','Re: Guest post for {{SITE_NAME}}?','Re: Guest post for {{SITE_NAME}}?'], bodies: [`Hi {{FIRSTNAME}},\n\nFound your site while researching packaging content — really liked your take on {{ARTICLE_TITLE}}. I work with Mill Packaging and we've put together some articles on custom packaging strategy, sustainable packaging options, and how unboxing impacts brand loyalty.\n\nOpen to us contributing a piece? We handle all the writing.\n\nBest,\nYam`,`Hey {{FIRSTNAME}}, just following up. Can send topic options your way if that's helpful — no pressure.\n\nYam`,`Hi {{FIRSTNAME}}, circling back here. We're flexible on angle and happy to cover any editorial costs on your side. Let me know if there's interest.\n\nYam`,`Closing the loop on this one. If guest content is something you consider in the future, feel free to reach out anytime.\n\nYam`] },
  'PRIMERX': { subjects: ['Article contribution for {{SITE_NAME}}','Re: Article contribution for {{SITE_NAME}}','Re: Article contribution for {{SITE_NAME}}','Re: Article contribution for {{SITE_NAME}}'], bodies: [`Hi {{FIRSTNAME}},\n\nYour article on {{ARTICLE_TITLE}} was a useful read — well done. I'm working with PrimeRx, a pharmacy management software platform, and we have some content around pharmacy automation, inventory optimization, and patient engagement tech that could be a fit for your readers.\n\nWould you be open to a contributed article? We handle all the drafting.\n\nBest,\nYam`,`Hey {{FIRSTNAME}}, floating this back to the top. Happy to share a few topic ideas so you can see if anything clicks.\n\nYam`,`Hi {{FIRSTNAME}}, wanted to follow up once more. We can write to your editorial standards and are glad to cover any editorial fees. Let me know.\n\nYam`,`Last note from me on this — no worries if it's not the right time. Open invitation stands if you need healthcare/pharmacy tech content in the future.\n\nYam`] },
  'SMOKEA': { subjects: ['Content idea for {{SITE_NAME}}','Re: Content idea for {{SITE_NAME}}','Re: Content idea for {{SITE_NAME}}','Re: Content idea for {{SITE_NAME}}'], bodies: [`Hi {{FIRSTNAME}},\n\nCame across your piece on {{ARTICLE_TITLE}} — good stuff. I work with SMOKEA (online smoke shop) and we have some content ready around topics like choosing the right glass, smoking accessory guides, and what's trending in the smoke shop space.\n\nInterested in a contributed article? We write everything — you just publish.\n\nCheers,\nYam`,`Hey {{FIRSTNAME}}, just following up on my last email. Happy to fire over topic ideas if that's easier.\n\nYam`,`Hi {{FIRSTNAME}}, checking in here. We're flexible on topics and glad to cover any editorial fees on your end. Would love to collaborate.\n\nYam`,`Last reach-out from me. If you're ever open to contributed content, feel free to hit me up.\n\nYam`] },
  'MRSKIN': { subjects: ['Collab idea — {{SITE_NAME}}','Re: Collab idea — {{SITE_NAME}}','Re: Collab idea — {{SITE_NAME}}','Re: Collab idea — {{SITE_NAME}}'], bodies: [`Hi {{FIRSTNAME}},\n\nYour {{ARTICLE_TITLE}} was a fun read — exactly my kind of content. I'm working with MrSkin (the go-to database for movie and TV scenes) and we have some article ideas around iconic movie moments, best scenes by genre, and celebrity filmography deep-dives.\n\nWould you be up for a contributed piece? We handle the writing.\n\nBest,\nYam`,`Hey {{FIRSTNAME}}, just bumping this. Can send over a few specific topic ideas if that helps you decide.\n\nYam`,`Hi {{FIRSTNAME}}, following up one more time. We can match your tone and style, and we're happy to cover any editorial or publishing fees. Let me know if there's room.\n\nYam`,`Last one from me — if entertainment content collabs make sense later, the offer stands.\n\nYam`] },
  'VRAI': { subjects: ['Article idea for {{SITE_NAME}}','Re: Article idea for {{SITE_NAME}}','Re: Article idea for {{SITE_NAME}}','Re: Article idea for {{SITE_NAME}}'], bodies: [`Hi {{FIRSTNAME}},\n\nReally enjoyed your piece on {{ARTICLE_TITLE}} — great perspective. I'm working with VRAI, a lab-grown diamond jewelry brand, and we have article ideas around lab-grown diamond education, sustainable luxury, and engagement ring trends that I think your audience would appreciate.\n\nWould you be open to a contributed article? We handle the writing.\n\nBest,\nYam`,`Hey {{FIRSTNAME}}, just circling back. Happy to share a few polished topic options for your review.\n\nYam`,`Hi {{FIRSTNAME}}, following up here. We write to your editorial guidelines and are happy to cover any editorial fees. Would love to collaborate.\n\nYam`,`Last message from me — if sustainability or jewelry content is ever needed, we're here.\n\nYam`] },
  'AMS-FULFILLMENT': { subjects: ['Guest contribution for {{SITE_NAME}}?','Re: Guest contribution for {{SITE_NAME}}?','Re: Guest contribution for {{SITE_NAME}}?','Re: Guest contribution for {{SITE_NAME}}?'], bodies: [`Hi {{FIRSTNAME}},\n\nFound your article on {{ARTICLE_TITLE}} while researching fulfillment content — well put together. I work with AMS Fulfillment (3PL provider) and we have articles on topics like scaling fulfillment operations, choosing a 3PL partner, and reducing shipping costs that would fit your audience.\n\nOpen to a contributed piece? We do all the writing.\n\nBest,\nYam`,`Hey {{FIRSTNAME}}, just following up. Can share topic ideas if that makes it easier to evaluate.\n\nYam`,`Hi {{FIRSTNAME}}, circling back once more. We write to your standards and are happy to cover any editorial costs. Let me know if there's interest.\n\nYam`,`Closing the loop here. If ecommerce/logistics content makes sense in the future, happy to reconnect.\n\nYam`] },
  'BUILT-BAR': { subjects: ['Article idea for {{SITE_NAME}}','Re: Article idea for {{SITE_NAME}}','Re: Article idea for {{SITE_NAME}}','Re: Article idea for {{SITE_NAME}}'], bodies: [`Hi {{FIRSTNAME}},\n\nYour post on {{ARTICLE_TITLE}} was great — practical and well-researched. I'm working with Built Bar (protein bar brand) and we have content around on-the-go nutrition, protein bar comparisons, and healthy snacking for active lifestyles that would resonate with your readers.\n\nInterested in a contributed article? We handle the writing.\n\nBest,\nYam`,`Hey {{FIRSTNAME}}, just bumping this. Happy to send topic ideas your way — no commitment needed upfront.\n\nYam`,`Hi {{FIRSTNAME}}, one more follow-up. We can tailor content to your style and are glad to cover any editorial fees. Let me know.\n\nYam`,`Last note from me — if fitness or nutrition content is something you're looking for later, feel free to reach out.\n\nYam`] },
  'NUTRABIO': { subjects: ['Contribution idea — {{SITE_NAME}}','Re: Contribution idea — {{SITE_NAME}}','Re: Contribution idea — {{SITE_NAME}}','Re: Contribution idea — {{SITE_NAME}}'], bodies: [`Hi {{FIRSTNAME}},\n\nBeen reading your content — your piece on {{ARTICLE_TITLE}} stood out. I work with NutraBio (sports nutrition brand known for full-label transparency) and we have articles on topics like supplement transparency, pre-workout science, and protein quality that would be a strong fit for your site.\n\nWould you be up for a contributed article? We handle the writing end.\n\nBest,\nYam`,`Hey {{FIRSTNAME}}, floating this back up. Can send over specific topic options if that helps.\n\nYam`,`Hi {{FIRSTNAME}}, following up once more. We write to your specs and are happy to cover any editorial fees on your end. Would love to make it work.\n\nYam`,`Closing the loop — if supplement or fitness content is ever needed, we're here. No pressure.\n\nYam`] },
  'VIVANTE-LIVING': { subjects: ['Content idea for {{SITE_NAME}}','Re: Content idea for {{SITE_NAME}}','Re: Content idea for {{SITE_NAME}}','Re: Content idea for {{SITE_NAME}}'], bodies: [`Hi {{FIRSTNAME}},\n\nYour piece on {{ARTICLE_TITLE}} caught my eye — beautiful editorial approach. I'm working with Vivante Living (luxury home goods and furniture brand) and we have content around interior styling trends, choosing statement furniture, and curating a luxury living space that would complement your site well.\n\nWould you be open to a contributed article? We handle the writing.\n\nBest,\nYam`,`Hey {{FIRSTNAME}}, just circling back. Happy to share a few refined topic ideas for your consideration.\n\nYam`,`Hi {{FIRSTNAME}}, following up once more. We write to your style and are glad to cover any editorial fees involved. Let me know if there's a fit.\n\nYam`,`Last reach-out from me. If home decor or luxury living content is ever on your radar, the offer's open.\n\nYam`] },
  'GOODR': { subjects: ['Quick idea for {{SITE_NAME}}','Re: Quick idea for {{SITE_NAME}}','Re: Quick idea for {{SITE_NAME}}','Re: Quick idea for {{SITE_NAME}}'], bodies: [`Hi {{FIRSTNAME}},\n\nLoved your post on {{ARTICLE_TITLE}} — right up my alley. I work with Goodr (the fun sunglasses brand built for runners and athletes) and we've got content ideas around running gear essentials, finding the right sports sunglasses, and staying stylish on the trail that would work great on your site.\n\nInterested in a contributed article? We take care of all the writing.\n\nCheers,\nYam`,`Hey {{FIRSTNAME}}, just bumping this. Happy to send a few specific topics your way — no strings attached.\n\nYam`,`Hi {{FIRSTNAME}}, following up one more time. We can match your voice and style, and are happy to cover any editorial fees. Would love to collaborate.\n\nYam`,`Last one from me — if running or outdoor gear content makes sense down the road, I'm a message away.\n\nYam`] },
};

// ─── Warming Schedule ──────────────────────────────────────────────────────────
const WARMING_SCHEDULE = { 1: 5, 2: 15, 3: 30, 4: 50 };
const SUBDOMAIN_CREATED = '2026-03-24';

const DRIP_STEPS = [
  { step: 1, delayDays: 0,  label: 'Initial Outreach', hour: 9 },
  { step: 2, delayDays: 3,  label: 'Follow-up 1',      hour: 10 },
  { step: 3, delayDays: 7,  label: 'Follow-up 2',      hour: 14 },
  { step: 4, delayDays: 14, label: 'Final Follow-up',   hour: 11 },
];

function getWarmingWeek() {
  const created = new Date(SUBDOMAIN_CREATED);
  const now = new Date();
  const daysSince = Math.floor((now - created) / (1000 * 60 * 60 * 24));
  return Math.min(4, Math.max(1, Math.ceil(daysSince / 7) || 1));
}

// ─── Utility ───────────────────────────────────────────────────────────────────
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function extractDomain(url) {
  try {
    const hostname = new URL(url).hostname.replace(/^www\./, '');
    return hostname;
  } catch { return null; }
}

function domainToSiteName(domain) {
  return domain
    .replace(/\.(com|org|net|io|co|us|live|ai|info|biz|edu|gov|me)$/i, '')
    .replace(/[-_.]/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase());
}

// ─── Brevo API ─────────────────────────────────────────────────────────────────
async function brevoFetch(endpoint, method = 'GET', body = null) {
  const opts = {
    method,
    headers: { 'api-key': BREVO_KEY, 'Content-Type': 'application/json', Accept: 'application/json' },
  };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(`https://api.brevo.com/v3${endpoint}`, opts);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Brevo ${method} ${endpoint} ${res.status}: ${text}`);
  }
  return res.json();
}

async function getBrevoListsByPrefix(prefix) {
  const lists = [];
  let offset = 0;
  const limit = 50;
  while (true) {
    const data = await brevoFetch(`/contacts/lists?limit=${limit}&offset=${offset}&sort=desc`);
    if (!data.lists || data.lists.length === 0) break;
    for (const l of data.lists) {
      if (l.name.startsWith(prefix)) lists.push(l);
    }
    offset += limit;
    if (offset >= (data.count || 0)) break;
  }
  return lists;
}

/**
 * Count unsent prospects for a client by checking Brevo campaign status.
 * Looks for scheduled (not yet sent) campaigns with the client slug prefix.
 * Returns estimated unsent contact count.
 */
async function countUnsentProspects(clientSlug) {
  let totalUnsent = 0;
  let offset = 0;
  const limit = 50;

  while (true) {
    const data = await brevoFetch(`/emailCampaigns?type=classic&status=scheduled&limit=${limit}&offset=${offset}&sort=desc`);
    if (!data.campaigns || data.campaigns.length === 0) break;

    for (const c of data.campaigns) {
      // Only count step-1 campaigns for this client (avoid counting follow-ups as separate prospects)
      if (c.name.startsWith(clientSlug) && c.name.includes('-SEQ1-')) {
        const recipients = c.recipients?.listIds || [];
        for (const listId of recipients) {
          try {
            const listData = await brevoFetch(`/contacts/lists/${listId}`);
            totalUnsent += listData.uniqueSubscribers || 0;
          } catch { /* list may be deleted */ }
        }
      }
    }

    offset += limit;
    if (offset >= (data.count || 0)) break;
  }

  return totalUnsent;
}

async function createContactList(name) {
  const data = await brevoFetch('/contacts/lists', 'POST', { name, folderId: 1 });
  if (data.id) return data.id;
  if (data.code === 'duplicate_parameter') {
    const lists = await brevoFetch('/contacts/lists?limit=50&offset=0');
    const match = lists.lists?.find(l => l.name === name);
    return match?.id;
  }
  throw new Error(`Failed to create list "${name}": ${JSON.stringify(data)}`);
}

async function importContacts(listId, contacts) {
  return brevoFetch('/contacts/import', 'POST', {
    listIds: [listId],
    jsonBody: contacts.map(c => ({
      email: c.email,
      attributes: {
        FIRSTNAME: c.firstName || 'there',
        DOMAIN: c.domain,
        SITE_NAME: c.siteName || c.domain,
        ARTICLE_TITLE: c.articleTitle || '',
        ARTICLE_URL: c.articleUrl || '',
      },
    })),
    updateExistingContacts: true,
    emptyContactsAttributes: false,
  });
}

async function createEmailCampaign(name, sender, listId, subject, htmlBody, scheduledAt) {
  return brevoFetch('/emailCampaigns', 'POST', {
    name,
    sender: { name: sender.name, email: sender.email, id: sender.id },
    recipients: { listIds: [listId] },
    subject,
    htmlContent: `<html><body><p style="font-family:Arial,sans-serif;font-size:14px;line-height:1.5">${htmlBody.replace(/\n/g, '<br>')}</p><p style="font-size:11px;color:#999;margin-top:30px;">If you'd prefer not to hear from us, <a href="{{ unsubscribe }}" style="color:#999;text-decoration:underline;">unsubscribe here</a>.</p></body></html>`,
    scheduledAt,
    inlineImageActivation: false,
  });
}

/**
 * Fetch ALL existing Brevo contact emails for global deduplication.
 * Checks across ALL lists (not just this client) — no email gets sent twice, ever.
 * Cached: only fetched once per run, shared across all clients.
 */
let _globalEmailCache = null;
async function getExistingEmails() {
  if (_globalEmailCache) return _globalEmailCache;
  const emails = new Set();
  let offset = 0;
  const limit = 500;
  while (true) {
    try {
      const data = await brevoFetch(`/contacts?limit=${limit}&offset=${offset}`);
      if (!data.contacts || data.contacts.length === 0) break;
      for (const c of data.contacts) {
        emails.add(c.email.toLowerCase());
      }
      offset += limit;
      if (offset >= (data.count || 0)) break;
      await sleep(300);
    } catch { break; }
  }
  _globalEmailCache = emails;
  return emails;
}

// ─── DataForSEO API ────────────────────────────────────────────────────────────
const DATAFORSEO_AUTH = () => Buffer.from(`${DATAFORSEO_LOGIN}:${DATAFORSEO_PASSWORD}`).toString('base64');

/**
 * Query DataForSEO SERP API for fresh prospect domains.
 * Batches 3 requests at a time with 2s delays.
 * Returns array of { domain, url, title } objects.
 */
async function scrapeSERPs(keywords) {
  const queries = [];
  for (const kw of keywords) {
    for (const suffix of SUFFIXES) {
      queries.push(`${kw} ${suffix}`);
    }
  }

  const results = [];
  const batchSize = 3;

  for (let i = 0; i < queries.length; i += batchSize) {
    const batch = queries.slice(i, i + batchSize);
    const tasks = batch.map(q => ({
      keyword: q,
      location_code: 2840,
      language_code: 'en',
      depth: 100,
    }));

    try {
      const res = await fetch('https://api.dataforseo.com/v3/serp/google/organic/live/advanced', {
        method: 'POST',
        headers: {
          'Authorization': `Basic ${DATAFORSEO_AUTH()}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(tasks),
      });

      const data = await res.json();

      if (data.tasks) {
        for (const task of data.tasks) {
          if (task.result) {
            for (const resultSet of task.result) {
              if (resultSet.items) {
                for (const item of resultSet.items) {
                  if (item.type === 'organic' && item.url) {
                    const domain = extractDomain(item.url);
                    if (domain) {
                      results.push({
                        domain,
                        url: item.url,
                        title: item.title || '',
                      });
                    }
                  }
                }
              }
            }
          }
        }
      }
    } catch (err) {
      console.error(`DataForSEO batch error: ${err.message}`);
    }

    if (i + batchSize < queries.length) await sleep(2000);
  }

  return results;
}

// ─── AnyMailFinder API ─────────────────────────────────────────────────────────
/**
 * Look up contact emails for domains via AnyMailFinder.
 * Batches 10 at a time with 1s delays.
 * Returns array of { domain, email, firstName } objects.
 */
async function lookupEmails(domains) {
  const results = [];
  const batchSize = 10;

  for (let i = 0; i < domains.length; i += batchSize) {
    const batch = domains.slice(i, i + batchSize);

    const lookups = batch.map(async (domain) => {
      try {
        const res = await fetch('https://api.anymailfinder.com/v5.0/search/company.json', {
          method: 'POST',
          headers: {
            'X-Api-Key': ANYMAILFINDER_KEY,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ domain }),
        });

        const data = await res.json();

        if (data.success && data.results?.emails?.length > 0) {
          // Pick first personal email (skip generic addresses)
          const generic = ['info@', 'contact@', 'support@', 'hello@', 'admin@', 'sales@', 'team@', 'help@'];
          const personal = data.results.emails.find(e => !generic.some(g => e.toLowerCase().startsWith(g)));
          const email = personal || data.results.emails[0];
          const namePart = email.split('@')[0].split('.')[0];
          const firstName = namePart.charAt(0).toUpperCase() + namePart.slice(1);
          return {
            domain,
            email,
            firstName: firstName || 'there',
          };
        }
        return null;
      } catch (err) {
        console.error(`AnyMailFinder error for ${domain}: ${err.message}`);
        return null;
      }
    });

    const batchResults = await Promise.all(lookups);
    for (const r of batchResults) {
      if (r) results.push(r);
    }

    if (i + batchSize < domains.length) await sleep(1000);
  }

  return results;
}

// ─── Airtable Cost Tracking ────────────────────────────────────────────────────
async function trackCostInAirtable(clientSlug, serpQueries, emailLookups, newProspects, estimatedCost) {
  if (!AIRTABLE_PAT || !AIRTABLE_BASE) return;

  try {
    const dateStr = new Date().toISOString().split('T')[0];
    await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE}/OutreachCosts`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${AIRTABLE_PAT}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        records: [{
          fields: {
            Date: dateStr,
            Client: clientSlug,
            DataForSEOQueries: serpQueries,
            EmailLookups: emailLookups,
            NewProspects: newProspects,
            EstimatedCost: estimatedCost,
          },
        }],
      }),
    });
  } catch (err) {
    console.error(`Airtable tracking error for ${clientSlug}: ${err.message}`);
  }
}

// ─── Push to Brevo (Campaign Creation) ─────────────────────────────────────────
async function pushToBrevoCampaigns(clientSlug, prospects, clientConfig, week) {
  const template = TEMPLATES[clientSlug];
  if (!template) throw new Error(`No template for ${clientSlug}`);

  const warmedLimit = 50;
  const newLimit = WARMING_SCHEDULE[Math.min(week, 4)];
  const dailyLimit = warmedLimit + newLimit;

  // Split prospects into daily batches
  const batches = [];
  for (let i = 0; i < prospects.length; i += dailyLimit) {
    batches.push(prospects.slice(i, i + dailyLimit));
  }

  const dateStr = new Date().toISOString().split('T')[0];
  let campaignsCreated = 0;

  for (let batchIdx = 0; batchIdx < batches.length; batchIdx++) {
    const batch = batches[batchIdx];
    const batchDay = batchIdx;

    const warmedBatch = batch.slice(0, warmedLimit);
    const newBatch = batch.slice(warmedLimit, warmedLimit + newLimit);

    for (const { sender, contacts: batchContacts, label } of [
      { sender: clientConfig.senderWarmed, contacts: warmedBatch, label: 'W' },
      { sender: clientConfig.senderNew, contacts: newBatch, label: 'N' },
    ]) {
      if (batchContacts.length === 0) continue;

      const listName = `${clientSlug}-${label}-D${batchIdx + 1}-${dateStr}-replenish`;
      const listId = await createContactList(listName);
      await sleep(300);

      const mappedContacts = batchContacts.map(p => ({
        email: p.email,
        firstName: p.firstName || 'there',
        domain: p.domain,
        siteName: domainToSiteName(p.domain),
        articleTitle: p.title || '',
        articleUrl: p.url || '',
      }));

      await importContacts(listId, mappedContacts);
      await sleep(300);

      // Create 4 drip campaigns for this batch
      const baseDate = new Date();
      baseDate.setDate(baseDate.getDate() + 1 + batchDay);

      for (let step = 0; step < 4; step++) {
        const ds = DRIP_STEPS[step];
        const scheduledAt = new Date(baseDate);
        scheduledAt.setDate(scheduledAt.getDate() + ds.delayDays);
        scheduledAt.setHours(ds.hour, 0, 0, 0);

        const campaignName = `${clientSlug}-${label}-D${batchIdx + 1}-SEQ${step + 1}-${dateStr}-replenish`;
        const result = await createEmailCampaign(
          campaignName, sender, listId,
          template.subjects[step], template.bodies[step],
          scheduledAt.toISOString(),
        );

        if (result.id) campaignsCreated++;
        await sleep(400);
      }
    }
  }

  return { batches: batches.length, campaignsCreated, dailyLimit };
}

// ─── Blocked Domains (never outreach to these) ────────────────────────────────
const BLOCKED_DOMAINS = new Set([
  'google.com', 'youtube.com', 'facebook.com', 'twitter.com', 'x.com',
  'instagram.com', 'linkedin.com', 'reddit.com', 'pinterest.com', 'tiktok.com',
  'amazon.com', 'ebay.com', 'walmart.com', 'wikipedia.org', 'wikimedia.org',
  'github.com', 'stackoverflow.com', 'medium.com', 'quora.com',
  'apple.com', 'microsoft.com', 'adobe.com', 'wordpress.com', 'wordpress.org',
  'shopify.com', 'squarespace.com', 'wix.com', 'weebly.com',
  'yelp.com', 'tripadvisor.com', 'bbb.org', 'craigslist.org',
  'gov', 'edu', // TLD blocks handled separately
]);

function isDomainBlocked(domain) {
  if (!domain) return true;
  if (BLOCKED_DOMAINS.has(domain)) return true;
  if (domain.endsWith('.gov') || domain.endsWith('.edu') || domain.endsWith('.mil')) return true;
  return false;
}

// ─── Main Pipeline per Client ──────────────────────────────────────────────────
async function processClient(clientConfig, week, globalDedupeSet) {
  const { slug, name, keywords } = clientConfig;
  const log = (msg) => console.log(`  [${slug}] ${msg}`);

  // Step 1: Check remaining unsent prospects
  log('Checking unsent prospects...');
  const remaining = await countUnsentProspects(slug);
  log(`Remaining unsent: ${remaining}`);

  if (remaining >= REPLENISH_THRESHOLD) {
    return { slug, name, status: 'skipped', remaining, newProspects: 0, reason: `${remaining} remaining >= ${REPLENISH_THRESHOLD} threshold` };
  }

  // Step 2: Get existing emails for global deduplication (all clients, all lists)
  log('Loading existing emails for dedup...');
  const existingEmails = await getExistingEmails();
  const existingDomains = new Set();
  for (const email of existingEmails) {
    const domain = email.split('@')[1];
    if (domain) existingDomains.add(domain);
  }
  log(`Found ${existingEmails.size} existing emails, ${existingDomains.size} domains to skip`);

  // Step 3: SERP scraping
  log(`Scraping SERPs (${keywords.length} keywords x ${SUFFIXES.length} suffixes = ${keywords.length * SUFFIXES.length} queries)...`);
  const serpResults = await scrapeSERPs(keywords);
  const serpQueryCount = keywords.length * SUFFIXES.length;
  log(`Got ${serpResults.length} raw SERP results`);

  // Step 4: Dedupe domains
  const uniqueDomains = new Map(); // domain -> { url, title }
  for (const r of serpResults) {
    if (isDomainBlocked(r.domain)) continue;
    if (existingDomains.has(r.domain)) continue;
    if (globalDedupeSet.has(r.domain)) continue;
    if (!uniqueDomains.has(r.domain)) {
      uniqueDomains.set(r.domain, { url: r.url, title: r.title });
    }
  }

  // Add to global dedupe set
  for (const domain of uniqueDomains.keys()) {
    globalDedupeSet.add(domain);
  }

  log(`${uniqueDomains.size} unique new domains after dedup`);

  if (uniqueDomains.size === 0) {
    return { slug, name, status: 'no_new', remaining, newProspects: 0, serpQueries: serpQueryCount, reason: 'No new domains found after dedup' };
  }

  // Step 5: Email lookup
  const domainsToLookup = Array.from(uniqueDomains.keys());
  log(`Looking up emails for ${domainsToLookup.length} domains...`);
  const emailResults = await lookupEmails(domainsToLookup);
  log(`Found ${emailResults.length} emails`);

  if (emailResults.length === 0) {
    return { slug, name, status: 'no_emails', remaining, newProspects: 0, serpQueries: serpQueryCount, emailLookups: domainsToLookup.length, reason: 'No emails found' };
  }

  // Step 6: Build prospect objects
  const prospects = emailResults.map(r => {
    const serpData = uniqueDomains.get(r.domain) || {};
    return {
      domain: r.domain,
      email: r.email,
      firstName: r.firstName || 'there',
      siteName: domainToSiteName(r.domain),
      title: serpData.title || '',
      url: serpData.url || '',
      hasEmail: true,
    };
  });

  // Step 7: Push to Brevo
  log(`Pushing ${prospects.length} prospects to Brevo...`);
  const pushResult = await pushToBrevoCampaigns(slug, prospects, clientConfig, week);
  log(`Created ${pushResult.campaignsCreated} campaigns across ${pushResult.batches} batches`);

  // Step 8: Track costs in Airtable
  const serpCost = serpQueryCount * 0.01; // ~$0.01 per query
  const emailCost = domainsToLookup.length * 0.01; // ~$0.01 per lookup
  const totalCost = serpCost + emailCost;
  await trackCostInAirtable(slug, serpQueryCount, domainsToLookup.length, prospects.length, totalCost);

  return {
    slug,
    name,
    status: 'replenished',
    remaining,
    newProspects: prospects.length,
    serpQueries: serpQueryCount,
    emailLookups: domainsToLookup.length,
    campaignsCreated: pushResult.campaignsCreated,
    estimatedCost: totalCost,
  };
}

// ─── Main Handler ──────────────────────────────────────────────────────────────
async function runReplenishment() {
  const startTime = Date.now();
  const week = getWarmingWeek();
  const dateStr = new Date().toISOString().split('T')[0];
  const globalDedupeSet = new Set();

  console.log(`=== Prospect Replenishment — ${dateStr} ===`);
  console.log(`Warming week: ${week} (new subdomain limit: ${WARMING_SCHEDULE[Math.min(week, 4)]}/day)`);

  // Step 1: Check remaining prospects for all clients to prioritize
  console.log('\nChecking remaining prospects for all clients...');
  const clientStatus = [];
  for (const config of CLIENT_CONFIGS) {
    try {
      const remaining = await countUnsentProspects(config.slug);
      clientStatus.push({ config, remaining });
    } catch (err) {
      console.error(`Error checking ${config.slug}: ${err.message}`);
      clientStatus.push({ config, remaining: 999, error: err.message });
    }
  }

  // Sort by remaining (ascending) — most urgent first
  clientStatus.sort((a, b) => a.remaining - b.remaining);

  console.log('Priority order:');
  for (const { config, remaining } of clientStatus) {
    const urgent = remaining < REPLENISH_THRESHOLD ? '** NEEDS REPLENISH **' : 'OK';
    console.log(`  ${config.slug}: ${remaining} remaining — ${urgent}`);
  }

  // Step 2: Process clients in priority order, respecting timeout
  const results = [];
  const deferred = [];

  for (const { config, remaining } of clientStatus) {
    const elapsed = Date.now() - startTime;
    if (elapsed > TIMEOUT_SAFETY_MS) {
      deferred.push({ slug: config.slug, name: config.name, remaining, reason: 'timeout' });
      continue;
    }

    console.log(`\n--- Processing ${config.slug} (${remaining} remaining) ---`);
    try {
      const result = await processClient(config, week, globalDedupeSet);
      results.push(result);
    } catch (err) {
      console.error(`Error processing ${config.slug}: ${err.message}`);
      results.push({ slug: config.slug, name: config.name, status: 'error', error: err.message, remaining });
    }
  }

  // Step 3: Build summary
  const totalSerpQueries = results.reduce((s, r) => s + (r.serpQueries || 0), 0);
  const totalEmailLookups = results.reduce((s, r) => s + (r.emailLookups || 0), 0);
  const totalNewProspects = results.reduce((s, r) => s + (r.newProspects || 0), 0);
  const totalCost = results.reduce((s, r) => s + (r.estimatedCost || 0), 0);

  let summary = `:package: *Weekly Prospect Replenishment — ${new Date().toLocaleDateString('en-US', { month: 'long', day: 'numeric' })}*\n\n`;

  for (const r of results) {
    if (r.status === 'replenished') {
      summary += `:white_check_mark: *${r.slug}:* +${r.newProspects} new prospects (was at ${r.remaining} remaining)\n`;
    } else if (r.status === 'skipped') {
      summary += `:fast_forward: *${r.slug}:* ${r.remaining} remaining — skipped\n`;
    } else if (r.status === 'no_new' || r.status === 'no_emails') {
      summary += `:warning: *${r.slug}:* ${r.reason} (was at ${r.remaining} remaining)\n`;
    } else if (r.status === 'error') {
      summary += `:x: *${r.slug}:* Error — ${r.error}\n`;
    }
  }

  if (deferred.length > 0) {
    summary += `\n:hourglass: *Deferred (timeout):*\n`;
    for (const d of deferred) {
      summary += `  ${d.slug}: ${d.remaining} remaining — will process next cycle\n`;
    }
  }

  summary += `\n:moneybag: *Costs this cycle:*\n`;
  summary += `- DataForSEO: ${totalSerpQueries} queries ($${(totalSerpQueries * 0.01).toFixed(2)})\n`;
  summary += `- AnyMailFinder: ${totalEmailLookups} lookups ($${(totalEmailLookups * 0.01).toFixed(2)})\n`;
  summary += `- Total: $${totalCost.toFixed(2)}\n`;
  summary += `- New prospects: ${totalNewProspects}`;

  console.log('\n' + summary);

  // Step 4: Post to Slack
  try {
    await slack.post(CHANNEL(), summary);
  } catch (err) {
    console.error(`Slack post failed: ${err.message}`);
  }

  return { results, deferred, totalNewProspects, totalCost };
}

// ─── Vercel Handler ────────────────────────────────────────────────────────────
module.exports = async function handler(req, res) {
  // Only allow GET (cron) and POST (manual trigger)
  if (req.method !== 'GET' && req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  // Verify cron secret if present (Vercel cron sends this)
  const cronSecret = process.env.CRON_SECRET;
  if (cronSecret && req.headers.authorization !== `Bearer ${cronSecret}`) {
    // Allow if no CRON_SECRET is set, or if it matches
    if (cronSecret) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
  }

  // Return immediately, process in background
  const promise = runReplenishment()
    .then(result => {
      console.log(`Replenishment complete: ${result.totalNewProspects} new prospects, $${result.totalCost.toFixed(2)} cost`);
    })
    .catch(err => {
      console.error('Replenishment failed:', err);
    });

  waitUntil(promise);

  res.status(200).json({
    status: 'processing',
    message: 'Prospect replenishment started. Results will be posted to Slack.',
    timestamp: new Date().toISOString(),
  });
};
