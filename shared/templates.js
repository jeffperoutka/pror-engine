/**
 * Email templates for outreach drip sequences — shared between prospect-replenish and drip-sender
 *
 * TEMPLATE PRINCIPLES (optimized for 60%+ open rate, 10%+ reply rate):
 * 1. Subject lines: Short (3-6 words), curiosity-driven, no "guest post" in subject
 * 2. Bodies: Value-first, specific article reference, clear CTA, under 80 words
 * 3. Follow-ups: Shorter each time, different angle, mention covering fees in step 3
 * 4. Sign off as Josh (Content Partnerships) — matches reply handler identity
 * 5. Preheader text embedded for inbox preview optimization
 */

const TEMPLATES = {
  'ABLE-AMMO': {
    subjects: [
      '{{SITE_NAME}} + outdoor content',
      'Re: {{SITE_NAME}} + outdoor content',
      'Re: {{SITE_NAME}} + outdoor content',
      'Re: {{SITE_NAME}} + outdoor content',
    ],
    preheaders: [
      'I have an article idea that fits your audience',
      'Just floating this back up',
      'Happy to cover any editorial fees',
      'Last note from me',
    ],
    bodies: [
      `Hi {{FIRSTNAME}},\n\nYour piece on {{ARTICLE_TITLE}} was a great read. I've been working with Able Ammo on content around range safety, ammunition selection, and hunting gear — and your audience feels like a strong fit.\n\nWould you be open to a contributed article? We handle the writing.\n\nJosh\nContent Partnerships`,
      `Hey {{FIRSTNAME}}, just floating this back up — we have a few article ideas ready to go. Happy to send topics over if that's easier.\n\nJosh`,
      `Hi {{FIRSTNAME}}, one more check-in. We're flexible on topics, match your editorial style, and happy to cover any editorial fees. Let me know.\n\nJosh`,
      `Last one from me — if the timing isn't right, no worries. The door's open whenever content collaborations make sense.\n\nJosh`,
    ],
  },
  'DR-DABBER': {
    subjects: [
      'Idea for {{SITE_NAME}}',
      'Re: Idea for {{SITE_NAME}}',
      'Re: Idea for {{SITE_NAME}}',
      'Re: Idea for {{SITE_NAME}}',
    ],
    preheaders: [
      'Vaporizer content that fits your audience',
      'Circling back on this',
      'We cover editorial fees',
      'Closing the loop',
    ],
    bodies: [
      `Hi {{FIRSTNAME}},\n\nBeen following your work — {{ARTICLE_TITLE}} was a great read. I work with Dr. Dabber and we have content on vaporizer tech, concentrate guides, and device comparisons that could work well for your readers.\n\nWould you be open to a contributed piece? We handle the writing.\n\nJosh\nContent Partnerships`,
      `Hey {{FIRSTNAME}}, circling back — happy to send a few topic ideas tailored to your site if that helps.\n\nJosh`,
      `Hi {{FIRSTNAME}}, checking in one last time. We adapt to your guidelines and are glad to cover any editorial fees. Let me know if there's an opening.\n\nJosh`,
      `Closing the loop — totally understand if it's not the right fit. Door's open if you need cannabis/wellness content down the road.\n\nJosh`,
    ],
  },
  'FELINA': {
    subjects: [
      'Thought this might fit {{SITE_NAME}}',
      'Re: Thought this might fit {{SITE_NAME}}',
      'Re: Thought this might fit {{SITE_NAME}}',
      'Re: Thought this might fit {{SITE_NAME}}',
    ],
    preheaders: [
      'Women\'s fashion content your readers would love',
      'We have topics ready to share',
      'Happy to cover any editorial costs',
      'No hard feelings if now isn\'t the time',
    ],
    bodies: [
      `Hi {{FIRSTNAME}},\n\nLove what you're doing with {{SITE_NAME}} — your post on {{ARTICLE_TITLE}} really resonated. I'm working with Felina (women's intimates & loungewear) and we'd love to contribute something around bra fitting, loungewear essentials, or body-positive fashion.\n\nWould you be interested? We write it, you publish it.\n\nJosh\nContent Partnerships`,
      `Hey {{FIRSTNAME}}, just bumping this — we have a few topics ready. Happy to share so you can pick what fits.\n\nJosh`,
      `Hi {{FIRSTNAME}}, one more nudge. We write to your style guidelines and are happy to cover any editorial fees. Would love to make this work.\n\nJosh`,
      `Last message from me — happy to reconnect whenever it makes sense.\n\nJosh`,
    ],
  },
  'MILL-PACKAGING': {
    subjects: [
      'Content idea for {{SITE_NAME}}',
      'Re: Content idea for {{SITE_NAME}}',
      'Re: Content idea for {{SITE_NAME}}',
      'Re: Content idea for {{SITE_NAME}}',
    ],
    preheaders: [
      'Packaging strategy content for your readers',
      'Topic ideas ready if you\'re interested',
      'We cover editorial costs on our end',
      'Open invitation if timing works later',
    ],
    bodies: [
      `Hi {{FIRSTNAME}},\n\nFound your site while researching packaging content — your take on {{ARTICLE_TITLE}} was solid. I work with Mill Packaging and we have content on sustainable packaging, unboxing experience strategy, and custom packaging for DTC brands.\n\nOpen to a contributed piece? We handle all the writing.\n\nJosh\nContent Partnerships`,
      `Hey {{FIRSTNAME}}, following up — I can send topic options your way if that's helpful. No pressure.\n\nJosh`,
      `Hi {{FIRSTNAME}}, circling back. We're flexible on angle and happy to cover any editorial costs on your side. Let me know if there's interest.\n\nJosh`,
      `Last note — if guest content makes sense down the road, feel free to reach out anytime.\n\nJosh`,
    ],
  },
  'PRIMERX': {
    subjects: [
      'Healthcare content for {{SITE_NAME}}',
      'Re: Healthcare content for {{SITE_NAME}}',
      'Re: Healthcare content for {{SITE_NAME}}',
      'Re: Healthcare content for {{SITE_NAME}}',
    ],
    preheaders: [
      'Pharmacy tech content that fits your audience',
      'A few topic ideas ready to share',
      'We cover editorial fees',
      'No worries if timing is off',
    ],
    bodies: [
      `Hi {{FIRSTNAME}},\n\nYour article on {{ARTICLE_TITLE}} was a useful read. I'm working with PrimeRx (pharmacy management software) and we have content around pharmacy automation, inventory optimization, and patient engagement tech that could work well for your readers.\n\nWould you be open to a contributed article? We handle the drafting.\n\nJosh\nContent Partnerships`,
      `Hey {{FIRSTNAME}}, floating this back up — happy to share a few topic ideas so you can see if anything clicks.\n\nJosh`,
      `Hi {{FIRSTNAME}}, following up once more. We write to your editorial standards and are glad to cover any editorial fees. Let me know.\n\nJosh`,
      `Last note from me — open invitation stands if you need healthcare/pharmacy tech content in the future.\n\nJosh`,
    ],
  },
  'SMOKEA': {
    subjects: [
      '{{SITE_NAME}} content collab?',
      'Re: {{SITE_NAME}} content collab?',
      'Re: {{SITE_NAME}} content collab?',
      'Re: {{SITE_NAME}} content collab?',
    ],
    preheaders: [
      'Smoke culture content ready to go',
      'Topic ideas ready if you\'re interested',
      'Happy to cover any editorial fees',
      'Door\'s open whenever',
    ],
    bodies: [
      `Hi {{FIRSTNAME}},\n\nCame across your piece on {{ARTICLE_TITLE}} — good stuff. I work with SMOKEA and we have content around glass selection guides, smoking accessory trends, and what's new in the smoke shop space.\n\nInterested in a contributed article? We write everything, you just publish.\n\nJosh\nContent Partnerships`,
      `Hey {{FIRSTNAME}}, following up — happy to fire over topic ideas if that's easier.\n\nJosh`,
      `Hi {{FIRSTNAME}}, checking in. We're flexible on topics and glad to cover any editorial fees on your end. Would love to collaborate.\n\nJosh`,
      `Last one from me — if you're ever open to contributed content, feel free to hit me up.\n\nJosh`,
    ],
  },
  'MRSKIN': {
    subjects: [
      'Entertainment piece for {{SITE_NAME}}?',
      'Re: Entertainment piece for {{SITE_NAME}}?',
      'Re: Entertainment piece for {{SITE_NAME}}?',
      'Re: Entertainment piece for {{SITE_NAME}}?',
    ],
    preheaders: [
      'Movie and TV content your readers will love',
      'Specific topic ideas ready to share',
      'We cover editorial fees',
      'Offer stands whenever timing works',
    ],
    bodies: [
      `Hi {{FIRSTNAME}},\n\nYour {{ARTICLE_TITLE}} was a fun read — exactly my kind of content. I work with MrSkin and we have article ideas around iconic movie moments, best scenes by genre, and celebrity filmography deep-dives.\n\nWould you be up for a contributed piece? We handle the writing.\n\nJosh\nContent Partnerships`,
      `Hey {{FIRSTNAME}}, bumping this — I can send over specific topic ideas if that helps you decide.\n\nJosh`,
      `Hi {{FIRSTNAME}}, following up one more time. We match your tone and style, and we're happy to cover any editorial fees. Let me know.\n\nJosh`,
      `Last one from me — if entertainment content collabs make sense later, the offer stands.\n\nJosh`,
    ],
  },
  'VRAI': {
    subjects: [
      'Sustainability piece for {{SITE_NAME}}',
      'Re: Sustainability piece for {{SITE_NAME}}',
      'Re: Sustainability piece for {{SITE_NAME}}',
      'Re: Sustainability piece for {{SITE_NAME}}',
    ],
    preheaders: [
      'Lab-grown diamond content your audience would appreciate',
      'Polished topic options ready',
      'We cover editorial fees',
      'Happy to reconnect whenever',
    ],
    bodies: [
      `Hi {{FIRSTNAME}},\n\nReally enjoyed your piece on {{ARTICLE_TITLE}} — great perspective. I'm working with VRAI (lab-grown diamond jewelry) and we have article ideas around sustainable luxury, lab-grown diamond education, and engagement ring trends that would fit your audience.\n\nWould you be open to a contributed article? We handle the writing.\n\nJosh\nContent Partnerships`,
      `Hey {{FIRSTNAME}}, circling back — happy to share a few polished topic options for your review.\n\nJosh`,
      `Hi {{FIRSTNAME}}, following up. We write to your editorial guidelines and are happy to cover any editorial fees. Would love to collaborate.\n\nJosh`,
      `Last message from me — if sustainability or jewelry content is ever needed, we're here.\n\nJosh`,
    ],
  },
  'AMS-FULFILLMENT': {
    subjects: [
      'Logistics content for {{SITE_NAME}}',
      'Re: Logistics content for {{SITE_NAME}}',
      'Re: Logistics content for {{SITE_NAME}}',
      'Re: Logistics content for {{SITE_NAME}}',
    ],
    preheaders: [
      'Fulfillment and ecommerce content that fits',
      'Topic ideas ready to share',
      'We cover editorial costs',
      'Open invitation if timing works later',
    ],
    bodies: [
      `Hi {{FIRSTNAME}},\n\nFound your article on {{ARTICLE_TITLE}} — well put together. I work with AMS Fulfillment (3PL provider) and we have articles on scaling fulfillment ops, choosing a 3PL partner, and reducing shipping costs that would fit your audience.\n\nOpen to a contributed piece? We do all the writing.\n\nJosh\nContent Partnerships`,
      `Hey {{FIRSTNAME}}, following up — can share topic ideas if that makes it easier to evaluate.\n\nJosh`,
      `Hi {{FIRSTNAME}}, circling back. We write to your standards and are happy to cover any editorial costs. Let me know.\n\nJosh`,
      `Closing the loop — if ecommerce/logistics content makes sense in the future, happy to reconnect.\n\nJosh`,
    ],
  },
  'BUILT-BAR': {
    subjects: [
      'Fitness content for {{SITE_NAME}}',
      'Re: Fitness content for {{SITE_NAME}}',
      'Re: Fitness content for {{SITE_NAME}}',
      'Re: Fitness content for {{SITE_NAME}}',
    ],
    preheaders: [
      'Nutrition content your readers will love',
      'Topic ideas ready — no commitment needed',
      'We cover editorial fees',
      'Open door if timing works later',
    ],
    bodies: [
      `Hi {{FIRSTNAME}},\n\nYour post on {{ARTICLE_TITLE}} was great — practical and well-researched. I'm working with Built Bar and we have content on on-the-go nutrition, protein bar comparisons, and healthy snacking for active lifestyles.\n\nInterested in a contributed article? We handle the writing.\n\nJosh\nContent Partnerships`,
      `Hey {{FIRSTNAME}}, bumping this — happy to send topic ideas your way. No commitment needed upfront.\n\nJosh`,
      `Hi {{FIRSTNAME}}, one more follow-up. We tailor content to your style and are glad to cover any editorial fees. Let me know.\n\nJosh`,
      `Last note — if fitness or nutrition content is something you're looking for later, feel free to reach out.\n\nJosh`,
    ],
  },
  'NUTRABIO': {
    subjects: [
      'Sports nutrition piece for {{SITE_NAME}}',
      'Re: Sports nutrition piece for {{SITE_NAME}}',
      'Re: Sports nutrition piece for {{SITE_NAME}}',
      'Re: Sports nutrition piece for {{SITE_NAME}}',
    ],
    preheaders: [
      'Supplement transparency content for your readers',
      'Specific topic options ready',
      'We cover editorial fees',
      'No pressure — standing offer',
    ],
    bodies: [
      `Hi {{FIRSTNAME}},\n\nBeen reading your content — {{ARTICLE_TITLE}} stood out. I work with NutraBio (sports nutrition, known for full-label transparency) and we have articles on supplement transparency, pre-workout science, and protein quality that would be a strong fit.\n\nWould you be up for a contributed article? We handle the writing.\n\nJosh\nContent Partnerships`,
      `Hey {{FIRSTNAME}}, floating this back up — can send over specific topic options if that helps.\n\nJosh`,
      `Hi {{FIRSTNAME}}, following up once more. We write to your specs and are happy to cover any editorial fees. Would love to make it work.\n\nJosh`,
      `Closing the loop — if supplement or fitness content is ever needed, we're here.\n\nJosh`,
    ],
  },
  'VIVANTE-LIVING': {
    subjects: [
      'Interior design piece for {{SITE_NAME}}',
      'Re: Interior design piece for {{SITE_NAME}}',
      'Re: Interior design piece for {{SITE_NAME}}',
      'Re: Interior design piece for {{SITE_NAME}}',
    ],
    preheaders: [
      'Luxury home content that complements your editorial',
      'Refined topic ideas for your review',
      'We cover editorial fees involved',
      'Open offer whenever timing works',
    ],
    bodies: [
      `Hi {{FIRSTNAME}},\n\nYour piece on {{ARTICLE_TITLE}} caught my eye — beautiful editorial approach. I'm working with Vivante Living (luxury home goods & furniture) and we have content on interior styling trends, statement furniture selection, and curating luxury living spaces.\n\nWould you be open to a contributed article? We handle the writing.\n\nJosh\nContent Partnerships`,
      `Hey {{FIRSTNAME}}, circling back — happy to share a few refined topic ideas for your consideration.\n\nJosh`,
      `Hi {{FIRSTNAME}}, following up. We write to your style and are glad to cover any editorial fees. Let me know if there's a fit.\n\nJosh`,
      `Last note — if home decor or luxury living content is ever on your radar, the offer's open.\n\nJosh`,
    ],
  },
  'GOODR': {
    subjects: [
      'Active lifestyle piece for {{SITE_NAME}}',
      'Re: Active lifestyle piece for {{SITE_NAME}}',
      'Re: Active lifestyle piece for {{SITE_NAME}}',
      'Re: Active lifestyle piece for {{SITE_NAME}}',
    ],
    preheaders: [
      'Running and outdoor content that fits your audience',
      'A few topics ready — no strings attached',
      'We cover editorial fees on our end',
      'Standing offer whenever it makes sense',
    ],
    bodies: [
      `Hi {{FIRSTNAME}},\n\nLoved your post on {{ARTICLE_TITLE}} — right up my alley. I work with Goodr (sunglasses brand built for runners and athletes) and we have content on running gear essentials, sports sunglasses selection, and staying stylish on the trail.\n\nInterested in a contributed article? We handle all the writing.\n\nJosh\nContent Partnerships`,
      `Hey {{FIRSTNAME}}, bumping this — happy to send a few specific topics your way. No strings attached.\n\nJosh`,
      `Hi {{FIRSTNAME}}, following up one more time. We match your voice and style, and are happy to cover any editorial fees. Would love to collaborate.\n\nJosh`,
      `Last one from me — if running or outdoor gear content makes sense down the road, I'm a message away.\n\nJosh`,
    ],
  },
};

/**
 * Fill template placeholders with contact data
 */
function fillTemplate(text, contact) {
  return text
    .replace(/\{\{FIRSTNAME\}\}/g, contact.firstName || 'there')
    .replace(/\{\{SITE_NAME\}\}/g, contact.siteName || contact.domain || '')
    .replace(/\{\{ARTICLE_TITLE\}\}/g, contact.articleTitle || 'your recent content')
    .replace(/\{\{DOMAIN\}\}/g, contact.domain || '');
}

/**
 * Build HTML email body from plain text template, with optional preheader for inbox preview
 */
function toHtml(text, preheader) {
  const escaped = text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const preheaderHtml = preheader
    ? `<div style="display:none;font-size:1px;color:#ffffff;line-height:1px;max-height:0px;max-width:0px;opacity:0;overflow:hidden;">${preheader}${'&zwnj;&nbsp;'.repeat(30)}</div>`
    : '';
  return `<html><body>${preheaderHtml}<p style="font-family:Arial,sans-serif;font-size:14px;line-height:1.6;color:#333;">${escaped.replace(/\n/g, '<br>')}</p></body></html>`;
}

module.exports = { TEMPLATES, fillTemplate, toHtml };
