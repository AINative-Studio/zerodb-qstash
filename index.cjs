/**
 * zerodb-qstash — CommonJS entry point.
 *
 * Drop-in Upstash QStash replacement with ZeroDB event stream.
 * Zero config: auto-provisions a free ZeroDB project on first use.
 */

'use strict';

const ZERODB_API_BASE = 'https://api.ainative.studio';
const INSTANT_DB_ENDPOINT = `${ZERODB_API_BASE}/api/v1/public/instant-db`;

async function httpRequest(url, options = {}) {
  const res = await fetch(url, {
    ...options,
    headers: { ...options.headers },
  });

  if (!res.ok) {
    const body = await res.text().catch(() => '');
    const err = new Error(`ZeroDB API error ${res.status}: ${body}`);
    err.statusCode = res.status;
    throw err;
  }

  const contentType = res.headers.get('content-type') || '';
  if (contentType.includes('application/json')) {
    return res.json();
  }
  return res;
}

function generateId() {
  return `msg_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
}

async function autoProvision(source) {
  const data = await httpRequest(INSTANT_DB_ENDPOINT, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ source: source || 'zerodb-qstash' }),
  });

  return {
    projectId: data.project_id,
    apiKey: data.api_key,
    claimUrl: data.claim_url || null,
  };
}

class Client {
  constructor(options = {}) {
    this._apiKey = options.token || process.env.ZERODB_API_KEY || process.env.QSTASH_TOKEN || null;
    this._projectId = options.projectId || process.env.ZERODB_PROJECT_ID || null;
    this._apiBase = options.baseUrl || ZERODB_API_BASE;
    this._silent = options.silent || false;
    this._provisioned = !!(this._apiKey && this._projectId);
    this._claimUrl = null;
  }

  async _ensureProvisioned() {
    if (this._provisioned) return;

    const creds = await autoProvision('zerodb-qstash');
    this._projectId = creds.projectId;
    this._apiKey = creds.apiKey;
    this._claimUrl = creds.claimUrl;
    this._provisioned = true;

    if (!this._silent && creds.claimUrl) {
      console.log(`\n  ZeroDB auto-provisioned! Claim your project:\n  ${creds.claimUrl}\n`);
    }
  }

  _buildUrl(path) {
    return `${this._apiBase}/v1/zerodb/${this._projectId}/database${path}`;
  }

  _headers(extra = {}) {
    return {
      'Content-Type': 'application/json',
      'X-API-Key': this._apiKey,
      ...extra,
    };
  }

  async publish(options = {}) {
    await this._ensureProvisioned();

    const {
      url,
      topic,
      body,
      headers,
      delay,
      retries,
      cron,
      deduplicationId,
      contentBasedDeduplication,
      method,
      callback,
    } = options;

    const messageId = generateId();
    const destination = topic || url || 'default';

    const event = {
      event_type: `qstash.message.${destination}`,
      data: {
        messageId,
        destination,
        body: typeof body === 'string' ? body : JSON.stringify(body),
        headers: headers || {},
        method: method || 'POST',
        delay: delay || 0,
        retries: retries !== undefined ? retries : 3,
        cron: cron || null,
        deduplicationId: deduplicationId || null,
        contentBasedDeduplication: contentBasedDeduplication || false,
        callback: callback || null,
      },
      timestamp: new Date().toISOString(),
    };

    if (cron) {
      const hookResult = await httpRequest(this._buildUrl('/hooks'), {
        method: 'POST',
        headers: this._headers(),
        body: JSON.stringify({
          event_type: `qstash.scheduled.${destination}`,
          webhook_url: url || callback || null,
          cron_expression: cron,
          payload: event.data,
          active: true,
        }),
      });

      return {
        messageId,
        scheduleId: hookResult.hook_id || hookResult.id || `sched_${messageId}`,
        cron,
        destination,
      };
    }

    const result = await httpRequest(this._buildUrl('/events'), {
      method: 'POST',
      headers: this._headers(),
      body: JSON.stringify(event),
    });

    return {
      messageId,
      eventId: result.event_id || result.id,
      destination,
    };
  }

  async publishJSON(options = {}) {
    const { body, ...rest } = options;
    return this.publish({
      ...rest,
      body: typeof body === 'string' ? body : JSON.stringify(body),
      headers: {
        ...rest.headers,
        'Content-Type': 'application/json',
      },
    });
  }

  get topics() {
    return {
      list: () => this._listTopics(),
      get: (name) => this._getTopic(name),
      create: (name, endpoints) => this._createTopic(name, endpoints),
      remove: (name) => this._removeTopic(name),
      addEndpoint: (name, endpoint) => this._addTopicEndpoint(name, endpoint),
      removeEndpoint: (name, endpoint) => this._removeTopicEndpoint(name, endpoint),
    };
  }

  async _listTopics() {
    await this._ensureProvisioned();

    const events = await httpRequest(this._buildUrl('/events?limit=1000'), {
      method: 'GET',
      headers: this._headers(),
    });

    const list = Array.isArray(events) ? events : events.events || [];
    const topicSet = new Set();
    for (const e of list) {
      const type = e.event_type || '';
      if (type.startsWith('qstash.message.')) {
        topicSet.add(type.replace('qstash.message.', ''));
      }
    }

    return Array.from(topicSet).map((name) => ({ name, endpoints: [] }));
  }

  async _getTopic(name) {
    const topics = await this._listTopics();
    return topics.find((t) => t.name === name) || { name, endpoints: [] };
  }

  async _createTopic(name, endpoints = []) {
    await this._ensureProvisioned();

    await httpRequest(this._buildUrl('/events'), {
      method: 'POST',
      headers: this._headers(),
      body: JSON.stringify({
        event_type: 'qstash.topic.created',
        data: { name, endpoints },
        timestamp: new Date().toISOString(),
      }),
    });

    return { name, endpoints };
  }

  async _removeTopic(name) {
    await this._ensureProvisioned();

    await httpRequest(this._buildUrl('/events'), {
      method: 'POST',
      headers: this._headers(),
      body: JSON.stringify({
        event_type: 'qstash.topic.removed',
        data: { name },
        timestamp: new Date().toISOString(),
      }),
    });

    return { deleted: true, name };
  }

  async _addTopicEndpoint(name, endpoint) {
    return this._createTopic(name, [endpoint]);
  }

  async _removeTopicEndpoint(name, endpoint) {
    await this._ensureProvisioned();

    await httpRequest(this._buildUrl('/events'), {
      method: 'POST',
      headers: this._headers(),
      body: JSON.stringify({
        event_type: 'qstash.topic.endpoint_removed',
        data: { name, endpoint },
        timestamp: new Date().toISOString(),
      }),
    });

    return { removed: true, name, endpoint };
  }

  get schedules() {
    return {
      list: () => this._listSchedules(),
      get: (id) => this._getSchedule(id),
      create: (options) => this._createSchedule(options),
      delete: (id) => this._deleteSchedule(id),
      pause: (id) => this._pauseSchedule(id),
      resume: (id) => this._resumeSchedule(id),
    };
  }

  async _listSchedules() {
    await this._ensureProvisioned();

    try {
      const hooks = await httpRequest(this._buildUrl('/hooks'), {
        method: 'GET',
        headers: this._headers(),
      });

      const list = Array.isArray(hooks) ? hooks : hooks.hooks || [];
      return list
        .filter((h) => h.cron_expression || (h.event_type && h.event_type.startsWith('qstash.scheduled.')))
        .map((h) => ({
          scheduleId: h.hook_id || h.id,
          cron: h.cron_expression,
          destination: h.webhook_url,
          body: h.payload,
          active: h.active !== false,
        }));
    } catch {
      return [];
    }
  }

  async _getSchedule(id) {
    const schedules = await this._listSchedules();
    return schedules.find((s) => s.scheduleId === id) || null;
  }

  async _createSchedule(options = {}) {
    return this.publish({ ...options, cron: options.cron || options.schedule });
  }

  async _deleteSchedule(id) {
    await this._ensureProvisioned();

    await httpRequest(this._buildUrl(`/hooks/${id}`), {
      method: 'DELETE',
      headers: this._headers(),
    });

    return { deleted: true, scheduleId: id };
  }

  async _pauseSchedule(id) {
    await this._ensureProvisioned();

    await httpRequest(this._buildUrl(`/hooks/${id}`), {
      method: 'PATCH',
      headers: this._headers(),
      body: JSON.stringify({ active: false }),
    });

    return { paused: true, scheduleId: id };
  }

  async _resumeSchedule(id) {
    await this._ensureProvisioned();

    await httpRequest(this._buildUrl(`/hooks/${id}`), {
      method: 'PATCH',
      headers: this._headers(),
      body: JSON.stringify({ active: true }),
    });

    return { resumed: true, scheduleId: id };
  }

  get messages() {
    return {
      list: (options) => this._listMessages(options),
      get: (id) => this._getMessage(id),
    };
  }

  async _listMessages(options = {}) {
    await this._ensureProvisioned();

    const { topic, limit } = options;
    let url = this._buildUrl(`/events?limit=${limit || 100}`);
    if (topic) {
      url += `&event_type=qstash.message.${topic}`;
    }

    const result = await httpRequest(url, {
      method: 'GET',
      headers: this._headers(),
    });

    const events = Array.isArray(result) ? result : result.events || [];
    return events.map((e) => ({
      messageId: e.data?.messageId || e.event_id || e.id,
      topic: (e.event_type || '').replace('qstash.message.', ''),
      body: e.data?.body || e.data,
      timestamp: e.timestamp || e.created_at,
      state: 'delivered',
    }));
  }

  async _getMessage(id) {
    const messages = await this._listMessages({ limit: 1000 });
    return messages.find((m) => m.messageId === id) || null;
  }
}

module.exports = { Client, createClient: (opts) => new Client(opts) };
