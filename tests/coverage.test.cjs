/**
 * Additional coverage tests for zerodb-qstash.
 *
 * Covers uncovered branches and methods: addTopicEndpoint, removeTopicEndpoint,
 * getSchedule, getMessage, auto-provisioning console output, publish with url
 * destination, and httpRequest non-JSON branch.
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
// topics.addEndpoint and topics.removeEndpoint
// ---------------------------------------------------------------------------

describe('topics.addEndpoint', () => {
  test('delegates to createTopic', async () => {
    pushMock(200, { event_id: 'add-ep-ev' });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const result = await c.topics.addEndpoint('my-topic', 'https://hook.example.com');

    expect(result.name).toBe('my-topic');
    expect(result.endpoints).toEqual(['https://hook.example.com']);
  });
});

describe('topics.removeEndpoint', () => {
  test('publishes endpoint_removed event', async () => {
    pushMock(200, { event_id: 'rm-ep-ev' });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const result = await c.topics.removeEndpoint('my-topic', 'https://hook.example.com');

    expect(result.removed).toBe(true);
    expect(result.name).toBe('my-topic');
    expect(result.endpoint).toBe('https://hook.example.com');
  });
});

// ---------------------------------------------------------------------------
// schedules.get
// ---------------------------------------------------------------------------

describe('schedules.get', () => {
  test('returns schedule by ID', async () => {
    pushMock(200, [
      { hook_id: 'h1', event_type: 'qstash.scheduled.reports', cron_expression: '0 9 * * *', active: true, webhook_url: 'https://x.com', payload: {} },
      { hook_id: 'h2', event_type: 'qstash.scheduled.billing', cron_expression: '0 0 * * *', active: false },
    ]);

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const sched = await c.schedules.get('h1');

    expect(sched).not.toBeNull();
    expect(sched.scheduleId).toBe('h1');
    expect(sched.cron).toBe('0 9 * * *');
  });

  test('returns null for non-existent schedule', async () => {
    pushMock(200, []);

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const sched = await c.schedules.get('nonexistent');

    expect(sched).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// messages.get
// ---------------------------------------------------------------------------

describe('messages.get', () => {
  test('returns message by ID', async () => {
    pushMock(200, [
      {
        event_id: 'ev-m1',
        event_type: 'qstash.message.signups',
        data: { messageId: 'msg-target', body: '{"x":1}' },
        timestamp: '2026-01-01T00:00:00Z',
      },
      {
        event_id: 'ev-m2',
        event_type: 'qstash.message.billing',
        data: { messageId: 'msg-other', body: '{}' },
        timestamp: '2026-01-02T00:00:00Z',
      },
    ]);

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const msg = await c.messages.get('msg-target');

    expect(msg).not.toBeNull();
    expect(msg.messageId).toBe('msg-target');
  });

  test('returns null for non-existent message', async () => {
    pushMock(200, []);

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const msg = await c.messages.get('nonexistent');

    expect(msg).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// publish with url destination (no topic)
// ---------------------------------------------------------------------------

describe('publish with url destination', () => {
  test('uses url as destination when no topic', async () => {
    pushMock(200, { event_id: 'ev-url' });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const result = await c.publish({ url: 'https://webhook.site/abc', body: 'payload' });

    expect(result.destination).toBe('https://webhook.site/abc');
  });
});

// ---------------------------------------------------------------------------
// publish with all options
// ---------------------------------------------------------------------------

describe('publish with all options', () => {
  test('includes delay, retries, deduplicationId, method, callback, contentBasedDeduplication', async () => {
    pushMock(200, { event_id: 'ev-full' });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const result = await c.publish({
      topic: 'full-opts',
      body: { data: 'test' },
      headers: { 'X-Custom': 'val' },
      delay: 5000,
      retries: 5,
      deduplicationId: 'dedup-123',
      contentBasedDeduplication: true,
      method: 'PUT',
      callback: 'https://callback.example.com',
    });

    expect(result.messageId).toMatch(/^msg_/);

    const [, opts] = globalThis.fetch.mock.calls[0];
    const body = JSON.parse(opts.body);
    expect(body.data.delay).toBe(5000);
    expect(body.data.retries).toBe(5);
    expect(body.data.deduplicationId).toBe('dedup-123');
    expect(body.data.contentBasedDeduplication).toBe(true);
    expect(body.data.method).toBe('PUT');
    expect(body.data.callback).toBe('https://callback.example.com');
  });
});

// ---------------------------------------------------------------------------
// publish with cron — url/callback paths and fallback scheduleId
// ---------------------------------------------------------------------------

describe('publish cron edge cases', () => {
  test('uses callback as webhook_url when no url', async () => {
    pushMock(200, { hook_id: 'hook-cb' });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const result = await c.publish({
      topic: 'sched',
      body: 'data',
      cron: '*/5 * * * *',
      callback: 'https://cb.example.com',
    });

    const [, opts] = globalThis.fetch.mock.calls[0];
    const body = JSON.parse(opts.body);
    expect(body.webhook_url).toBe('https://cb.example.com');
  });

  test('fallback scheduleId when no hook_id or id in response', async () => {
    pushMock(200, { status: 'created' });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const result = await c.publish({
      topic: 'sched2',
      body: 'x',
      cron: '0 * * * *',
    });

    expect(result.scheduleId).toMatch(/^sched_msg_/);
  });

  test('uses response.id as scheduleId fallback', async () => {
    pushMock(200, { id: 'resp-id-123' });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const result = await c.publish({
      topic: 'sched3',
      body: 'y',
      cron: '0 0 * * *',
    });

    expect(result.scheduleId).toBe('resp-id-123');
  });
});

// ---------------------------------------------------------------------------
// schedules.create with schedule key instead of cron
// ---------------------------------------------------------------------------

describe('schedules.create with schedule key', () => {
  test('uses options.schedule as cron', async () => {
    pushMock(200, { hook_id: 'sched-alt' });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const result = await c.schedules.create({
      topic: 'alt-cron',
      body: 'test',
      schedule: '0 12 * * *',
    });

    expect(result.scheduleId).toBe('sched-alt');
    expect(result.cron).toBe('0 12 * * *');
  });
});

// ---------------------------------------------------------------------------
// Auto-provisioning with console output (non-silent)
// ---------------------------------------------------------------------------

describe('auto-provisioning with console output', () => {
  test('logs claim URL when not silent', async () => {
    const consoleSpy = jest.spyOn(console, 'log').mockImplementation();

    pushMock(200, {
      project_id: 'loud-proj',
      api_key: 'loud-key',
      claim_url: 'https://zerodb.ai/claim/loud',
    });
    pushMock(200, { event_id: 'ev-loud' });

    const c = new Client(); // not silent
    await c.publish({ topic: 'test', body: 'hello' });

    expect(consoleSpy).toHaveBeenCalledWith(
      expect.stringContaining('ZeroDB auto-provisioned')
    );

    consoleSpy.mockRestore();
  });

  test('does not log when no claimUrl', async () => {
    const consoleSpy = jest.spyOn(console, 'log').mockImplementation();

    pushMock(200, {
      project_id: 'quiet-proj',
      api_key: 'quiet-key',
    });
    pushMock(200, { event_id: 'ev-quiet' });

    const c = new Client(); // not silent, but no claim_url
    await c.publish({ topic: 'test', body: 'hi' });

    expect(consoleSpy).not.toHaveBeenCalled();

    consoleSpy.mockRestore();
  });
});

// ---------------------------------------------------------------------------
// httpRequest non-JSON response
// ---------------------------------------------------------------------------

describe('httpRequest branches', () => {
  test('returns raw response for non-JSON content type', async () => {
    pushMock(200, 'OK', 'text/plain');

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    // schedules.delete makes a DELETE request - the response is non-JSON
    // This triggers the non-JSON branch in httpRequest
    const result = await c.schedules.delete('test-id');
    expect(result.deleted).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// _listTopics with events.events format
// ---------------------------------------------------------------------------

describe('_listTopics with events object format', () => {
  test('handles events.events format', async () => {
    pushMock(200, {
      events: [
        { event_type: 'qstash.message.orders', data: {} },
      ],
    });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const topics = await c.topics.list();

    expect(topics).toHaveLength(1);
    expect(topics[0].name).toBe('orders');
  });
});

// ---------------------------------------------------------------------------
// _listTopics: topic.get for non-existent topic
// ---------------------------------------------------------------------------

describe('topics.get non-existent', () => {
  test('returns default topic when not found', async () => {
    pushMock(200, []);

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const topic = await c.topics.get('missing');

    expect(topic.name).toBe('missing');
    expect(topic.endpoints).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// _listSchedules with hooks.hooks format
// ---------------------------------------------------------------------------

describe('_listSchedules with hooks object format', () => {
  test('handles hooks.hooks format', async () => {
    pushMock(200, {
      hooks: [
        { hook_id: 'h1', cron_expression: '0 0 * * *', active: true },
      ],
    });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const scheds = await c.schedules.list();

    expect(scheds).toHaveLength(1);
    expect(scheds[0].scheduleId).toBe('h1');
  });
});

// ---------------------------------------------------------------------------
// _listMessages with result.events format and various data shapes
// ---------------------------------------------------------------------------

describe('_listMessages edge cases', () => {
  test('handles messages without topic filter', async () => {
    pushMock(200, {
      events: [
        {
          event_id: 'ev-1',
          event_type: 'qstash.message.general',
          data: { body: 'test data' },
          created_at: '2026-06-01',
        },
      ],
    });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const msgs = await c.messages.list({});

    expect(msgs).toHaveLength(1);
    expect(msgs[0].topic).toBe('general');
    expect(msgs[0].timestamp).toBe('2026-06-01');
  });

  test('handles messages with limit', async () => {
    pushMock(200, []);

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const msgs = await c.messages.list({ limit: 10 });

    expect(msgs).toEqual([]);

    const [url] = globalThis.fetch.mock.calls[0];
    expect(url).toContain('limit=10');
  });

  test('handles messages with fallback data fields', async () => {
    pushMock(200, [
      {
        event_id: 'ev-fallback',
        event_type: '',
        data: 'raw-data',
      },
    ]);

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const msgs = await c.messages.list({});

    expect(msgs[0].messageId).toBe('ev-fallback');
    expect(msgs[0].body).toBe('raw-data');
  });
});

// ---------------------------------------------------------------------------
// publish with retries=0 (falsy but defined)
// ---------------------------------------------------------------------------

describe('publish retries edge case', () => {
  test('retries=0 is preserved (not defaulted to 3)', async () => {
    pushMock(200, { event_id: 'ev-r0' });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    await c.publish({ topic: 'retry-test', body: 'x', retries: 0 });

    const [, opts] = globalThis.fetch.mock.calls[0];
    const body = JSON.parse(opts.body);
    expect(body.data.retries).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// publish event result with result.id fallback
// ---------------------------------------------------------------------------

describe('publish event result fallbacks', () => {
  test('uses result.id when no event_id', async () => {
    pushMock(200, { id: 'fallback-id' });

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const result = await c.publish({ topic: 'fb', body: 'data' });

    expect(result.eventId).toBe('fallback-id');
  });
});

// ---------------------------------------------------------------------------
// _listSchedules schedule with hook.id (not hook_id)
// ---------------------------------------------------------------------------

describe('_listSchedules id fallback', () => {
  test('uses hook.id when no hook_id', async () => {
    pushMock(200, [
      { id: 'alt-id', cron_expression: '0 6 * * *', active: true, webhook_url: 'https://x.com' },
    ]);

    const c = new Client({ token: 'k', projectId: 'p', silent: true });
    const scheds = await c.schedules.list();

    expect(scheds[0].scheduleId).toBe('alt-id');
  });
});

// ---------------------------------------------------------------------------
// baseUrl override
// ---------------------------------------------------------------------------

describe('Client baseUrl override', () => {
  test('uses custom baseUrl', () => {
    const c = new Client({ token: 'k', projectId: 'p', baseUrl: 'https://custom.api.com', silent: true });
    expect(c._apiBase).toBe('https://custom.api.com');
  });
});
