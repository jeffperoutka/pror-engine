# PROR Engine

Unified AI engine for PROR Marketing — link building, Reddit strategy, QA, client management.

## Architecture
Single Vercel project, one Slack app, all agents share the same brain.

### Agents
- `agents/link-builder.js` — Consolidated link building pipeline (discover → emails → copy → Brevo → send)
- `agents/reddit-strategist.js` — Reddit comment/post generation with brand alignment
- `agents/qa-checker.js` — Quality assurance for links and Reddit
- `agents/orchestrator.js` — Client management, status, finances, initiation coordination

### Shared Modules
- `shared/slack.js` — All Slack API interactions (post, update, stream, react, upload)
- `shared/ai.js` — Claude API wrapper (complete, chat, json, generate)
- `shared/airtable.js` — Data layer (clients, links, reddit, campaigns, finances)
- `shared/google.js` — Google Drive/Docs/Sheets via service account
- `shared/brain.js` — Natural language routing + context-aware responses

### API Endpoints
- `api/slack/events.js` — Slack Events (mentions, messages)
- `api/slack/commands.js` — Slash commands (/links, /reddit, /qa, /initiation, /clients, /current-status, /finances)
- `api/slack/interact.js` — Button clicks, modal submissions
- `api/health.js` — Health check

### Slash Commands
- `/links [client] [count] [dr40+]` — Run link building pipeline
- `/reddit [client] [count]` — Generate Reddit comments
- `/qa [client] [type]` — Run QA on links or reddit
- `/initiation [client]` — Full client initiation
- `/clients` — List active clients
- `/current-status [client]` — Client status + metrics
- `/finances [client]` — P&L summary

## Development
- `git push origin main` triggers Vercel deployment
- No build step — raw Node.js serverless functions
- All env vars in Vercel dashboard
- Test: `node -e "require('./shared/ai')"`

## Required Environment Variables
See `.env.example` for full list.

## Rules
- `rules/reddit.json` — Reddit generation rules (scope, uniqueness)
- Rules are loaded at agent init and injected into system prompts
