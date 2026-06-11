/**
 * zerodb-qstash unit tests.
 *
 * All HTTP calls are mocked via globalThis.fetch — no real API calls.
 */

const { Client, createClient } = require('../index.cjs');

// ---------------------------------------------------------------------------
// Mock fetch
// ---------------------------------------------------------------------------

const mockResponses = [];

function pushMock(status, body, contentType = 'application/json') {
  mockResponses.push({ status, body, contentType });
}

function createMockFetch() {
  return jest.fn(async (url, opts) => {
    const mock = mockResponses.shift();
    if (!mock) throw new Error(`Unexpected fetch call: ${url}`);

    return {
      ok: mock.status >= 200 && mock.status < 300,
      status: mock.status,
      headers: {
        get: (name) => {
          if (name === 'content-type') return mock.contentType;
          return null;
        },
      },
      json: async () => (typeof mock.body === 'string' ? JSON.parse(mock.body) : mock.body),
      text: async () => (typeof mock.body === 'string' ? mock.body : JSON.stringify(mock.body)),
    };
  });
}

beforeEach(() => {
  mockResponses.length = 0;
  globalThis.fetch = createMockFetch();
});

afterEach(() => {
  delete globalThis.fetch;
});

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

describe('Client constructor', () => {
  test('uses provided token', () => {
    const c = new Client({ token: 'tok-1', projectId: 'proj-1', silent: true });
    expect(c._apiKey).toBe('tok-1');
    expect(c._projectId).toBe('proj-1');
    expect(c._provisioned).toBe(true);
  });

  test('reads ZERODB_API_KEY from env', () => {
    process.env.ZERODB_API_KEY = 'env-key';
    process.env.ZERODB_PROJECT_ID = 'env-proj';

    const c = new Client({ silent: true });
    expect(c._apiKey).toBe('env-key');
    expect(c._projectId).toBe('env-proj');

    delete process.env.ZERODB_API_KEY;
    delete process.env.ZERODB_PROJECT_ID;
  });

  test('reads QSTASH_TOKEN for compat', () => {
    process.env.QSTASH_TOKEN = 'qs-tok';

    const c = new Client({ silent: true });
    expect(c._apiKey).toBe('qs-tok');

    delete process.env.QSTASH_TOKEN;
  });

  test('not provisioned when no credentials', () => {
    const c = new Client({ silent: true });
    expect(c._provisioned).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// createClient factory
// ---------------------------------------------------------------------------

describe('createClient', () => {
  test('returns Client instance', () => {
    const c = createClient({ token: 'k', projectId: 'p', silent: true });
    expect(c).toBeInstanceOf(Client);
  });
});

// ---------------------------------------------------------------------------
// Auto-provisioning
// ---------------------------------------------------------------------------

describe('auto-provisioning', () => {
  test('provisions on first publish', async () => {
    pushMock(200, { project_id: 'auto-proj', api_key: 'auto-key', claim_url: 'https://zerodb.ai/claim/x' });
    pushMock(200, { event_id: 'ev-1' });

    const c = new Client({ silent: true });
    await c.publish({ topic: 'test', body: 'hello' });

    expect(c._projectId).toBe('auto-proj');
    expect(c._apiKey).toBe('auto-key');
    expect(c._provisioned).toBe(true);
  });

  test('skips provisioning with existing credentials', async () => {
    pushMock(200, { event_id: 'ev-2' });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    await c.publish({ topic: 'test', body: 'hi' });

    expect(globalThis.fetch).toHaveBeenCalledTimes(1);
  });
});

// ---------------------------------------------------------------------------
// publish
// ---------------------------------------------------------------------------

describe('publish', () => {
  test('publishes to event stream', async () => {
    pushMock(200, { event_id: 'ev-3' });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const result = await c.publish({ topic: 'signups', body: { userId: '123' } });

    expect(result.messageId).toMatch(/^msg_/);
    expect(result.eventId).toBe('ev-3');
    expect(result.destination).toBe('signups');
  });

  test('defaults destination to "default"', async () => {
    pushMock(200, { event_id: 'ev-4' });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const result = await c.publish({ body: 'data' });

    expect(result.destination).toBe('default');
  });

  test('publishes with string body', async () => {
    pushMock(200, { event_id: 'ev-5' });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const result = await c.publish({ topic: 'logs', body: 'plain text' });

    expect(result.eventId).toBe('ev-5');
  });
});

// ---------------------------------------------------------------------------
// publish with cron (schedule)
// ---------------------------------------------------------------------------

describe('publish with cron', () => {
  test('registers a hook for scheduled messages', async () => {
    pushMock(200, { hook_id: 'hook-cron-1' });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const result = await c.publish({
      topic: 'daily-report',
      body: 'generate',
      cron: '0 9 * * *',
    });

    expect(result.scheduleId).toBe('hook-cron-1');
    expect(result.cron).toBe('0 9 * * *');
    expect(result.destination).toBe('daily-report');

    const [url, opts] = globalThis.fetch.mock.calls[0];
    expect(url).toContain('/hooks');
    const body = JSON.parse(opts.body);
    expect(body.cron_expression).toBe('0 9 * * *');
  });
});

// ---------------------------------------------------------------------------
// publishJSON
// ---------------------------------------------------------------------------

describe('publishJSON', () => {
  test('serializes body as JSON', async () => {
    pushMock(200, { event_id: 'ev-json' });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const result = await c.publishJSON({
      topic: 'users',
      body: { name: 'Alice', email: 'alice@example.com' },
    });

    expect(result.eventId).toBe('ev-json');
  });

  test('handles string body', async () => {
    pushMock(200, { event_id: 'ev-json-str' });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const result = await c.publishJSON({ topic: 'logs', body: 'already a string' });

    expect(result.eventId).toBe('ev-json-str');
  });
});

// ---------------------------------------------------------------------------
// Topics
// ---------------------------------------------------------------------------

describe('topics', () => {
  test('list extracts topics from events', async () => {
    pushMock(200, [
      { event_type: 'qstash.message.signups', data: {} },
      { event_type: 'qstash.message.signups', data: {} },
      { event_type: 'qstash.message.billing', data: {} },
      { event_type: 'other.event', data: {} },
    ]);

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const topics = await c.topics.list();

    expect(topics).toHaveLength(2);
    expect(topics.map((t) => t.name).sort()).toEqual(['billing', 'signups']);
  });

  test('get returns topic by name', async () => {
    pushMock(200, [{ event_type: 'qstash.message.users', data: {} }]);

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const topic = await c.topics.get('users');

    expect(topic.name).toBe('users');
  });

  test('create publishes topic.created event', async () => {
    pushMock(200, { event_id: 'topic-ev' });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const topic = await c.topics.create('notifications', ['https://example.com/hook']);

    expect(topic.name).toBe('notifications');
    expect(topic.endpoints).toEqual(['https://example.com/hook']);
  });

  test('remove publishes topic.removed event', async () => {
    pushMock(200, { event_id: 'rm-ev' });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const result = await c.topics.remove('old-topic');

    expect(result.deleted).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Schedules
// ---------------------------------------------------------------------------

describe('schedules', () => {
  test('list returns filtered hooks', async () => {
    pushMock(200, [
      { hook_id: 'h1', event_type: 'qstash.scheduled.reports', cron_expression: '0 9 * * *', active: true },
      { hook_id: 'h2', event_type: 'other.hook' },
    ]);

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const schedules = await c.schedules.list();

    expect(schedules).toHaveLength(1);
    expect(schedules[0].scheduleId).toBe('h1');
    expect(schedules[0].cron).toBe('0 9 * * *');
  });

  test('create delegates to publish with cron', async () => {
    pushMock(200, { hook_id: 'sched-new' });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const result = await c.schedules.create({
      topic: 'weekly',
      body: 'do it',
      cron: '0 0 * * 0',
    });

    expect(result.scheduleId).toBe('sched-new');
  });

  test('delete sends DELETE to hooks', async () => {
    pushMock(200, { deleted: true });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const result = await c.schedules.delete('h1');

    expect(result.deleted).toBe(true);
    expect(result.scheduleId).toBe('h1');
  });

  test('pause sends PATCH with active=false', async () => {
    pushMock(200, { ok: true });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const result = await c.schedules.pause('h1');

    expect(result.paused).toBe(true);
    const [, opts] = globalThis.fetch.mock.calls[0];
    expect(JSON.parse(opts.body).active).toBe(false);
  });

  test('resume sends PATCH with active=true', async () => {
    pushMock(200, { ok: true });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const result = await c.schedules.resume('h1');

    expect(result.resumed).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

describe('messages', () => {
  test('list returns formatted messages', async () => {
    pushMock(200, [
      {
        event_id: 'ev-m1',
        event_type: 'qstash.message.signups',
        data: { messageId: 'msg-1', body: '{"user":"a"}' },
        timestamp: '2026-01-01T00:00:00Z',
      },
    ]);

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const msgs = await c.messages.list({ topic: 'signups' });

    expect(msgs).toHaveLength(1);
    expect(msgs[0].messageId).toBe('msg-1');
    expect(msgs[0].topic).toBe('signups');
  });
});

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

describe('error handling', () => {
  test('API errors include status code', async () => {
    pushMock(401, { error: 'Unauthorized' });

    const c = new Client({ token: 'bad', projectId: 'p', silent: true });
    await expect(c.publish({ topic: 't', body: 'x' })).rejects.toThrow('ZeroDB API error 401');
  });

  test('schedules.list returns empty on error', async () => {
    pushMock(500, { error: 'Server error' });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const schedules = await c.schedules.list();
    expect(schedules).toEqual([]);
  });
});
