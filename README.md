# zerodb-qstash

Drop-in Upstash QStash replacement backed by ZeroDB event stream. Zero config — auto-provisions on first use.

## Why?

QStash is great for serverless messaging, but it's a separate service with its own billing. `zerodb-qstash` gives you the same `Client` API backed by ZeroDB's managed event stream. No signup, no separate account — just `npm install` and go.

## Install

```bash
npm install zerodb-qstash
```

## Quick Start

```javascript
import { Client } from 'zerodb-qstash';

const qstash = new Client({ token: process.env.ZERODB_API_KEY });

// Publish a message
await qstash.publish({
  topic: 'user-signups',
  body: { userId: '123', email: 'user@example.com' },
});

// Publish JSON (convenience)
await qstash.publishJSON({
  topic: 'notifications',
  body: { type: 'welcome', userId: '123' },
});

// Schedule recurring
await qstash.publish({
  topic: 'daily-report',
  body: 'generate',
  cron: '0 9 * * *',
});
```

That's it. On first use, a free ZeroDB project is auto-provisioned. You'll see a claim URL in the console to keep it permanently.

## QStash Migration Guide

Replace `@upstash/qstash` with `zerodb-qstash`:

```diff
- import { Client } from '@upstash/qstash';
+ import { Client } from 'zerodb-qstash';

- const qstash = new Client({ token: process.env.QSTASH_TOKEN });
+ const qstash = new Client({ token: process.env.ZERODB_API_KEY });
  // QSTASH_TOKEN also works for zero-change migration

// publish() works the same way
await qstash.publish({
  topic: 'emails',
  body: JSON.stringify({ to: 'user@example.com' }),
});

// publishJSON() works the same way
await qstash.publishJSON({
  topic: 'emails',
  body: { to: 'user@example.com', subject: 'Welcome' },
});
```

### What's different?

| Feature | QStash | zerodb-qstash |
|---------|--------|--------------|
| Setup | Upstash account + billing | Zero config, auto-provisions |
| Auth | `QSTASH_TOKEN` | `ZERODB_API_KEY` (or `QSTASH_TOKEN`) |
| publish() | HTTP delivery | Event stream + hooks |
| publishJSON() | Same | Same |
| Topics | Topic management | Same API |
| Schedules | Cron schedules | Same API |
| Messages | Message query | Same API |
| Dependencies | Several | Zero (native fetch) |

## Topics

```javascript
// Create a topic
await qstash.topics.create('notifications', ['https://app.com/hook']);

// List topics
const topics = await qstash.topics.list();

// Get a topic
const topic = await qstash.topics.get('notifications');

// Remove a topic
await qstash.topics.remove('old-topic');
```

## Schedules

```javascript
// Create a schedule
const schedule = await qstash.schedules.create({
  topic: 'cleanup',
  body: 'run',
  cron: '0 0 * * *', // daily at midnight
});

// List schedules
const schedules = await qstash.schedules.list();

// Pause / resume
await qstash.schedules.pause(schedule.scheduleId);
await qstash.schedules.resume(schedule.scheduleId);

// Delete
await qstash.schedules.delete(schedule.scheduleId);
```

## Messages

```javascript
// List messages
const messages = await qstash.messages.list({ topic: 'signups', limit: 50 });

// Get a message
const msg = await qstash.messages.get('msg_123');
```

## Configuration

```javascript
const qstash = new Client({
  token: 'your-zerodb-api-key',      // or env ZERODB_API_KEY / QSTASH_TOKEN
  projectId: 'your-project-id',      // or env ZERODB_PROJECT_ID
  baseUrl: 'https://api.ainative.studio', // default
  silent: false,                      // suppress console output
});
```

## CommonJS

```javascript
const { Client } = require('zerodb-qstash');
const qstash = new Client();
```

---

**ZeroDB** is an AI-native database with vectors, NoSQL, files, events, and Postgres — all auto-provisioned.

Get a free database instantly at [zerodb.ai](https://zerodb.ai) | [Docs](https://docs.ainative.studio) | [npm](https://www.npmjs.com/package/zerodb-qstash)

---

## Zero-Config Auto-Provisioning

**No account needed to start.** On first run, this package automatically:

1. Provisions a **free ZeroDB instance** (~800ms)
2. Saves credentials locally (`.env`, `.mcp.json`)
3. Prints a **claim link** in your terminal

### The 72-Hour Window

Your auto-provisioned instance is **free for 72 hours**. To keep your data permanently, visit the **claim URL** printed in your terminal.

### For AI Agents

If a coding agent (Claude Code, Cursor, Windsurf) installs this package:
- Auto-provisions without human intervention
- The **claim URL** appears in agent output — visit it to take ownership
- `.claude/CLAUDE.md` instructs agents to surface the claim link to you
- Check `.env` or `~/.ainative/config.json` for your project ID if you miss it
