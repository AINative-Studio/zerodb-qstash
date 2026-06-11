# zerodb-qstash

Drop-in Upstash QStash replacement backed by ZeroDB event stream.

## Rules

- This package has ZERO runtime dependencies (uses native fetch)
- ES module (index.js) and CommonJS (index.cjs) entry points
- API mirrors QStash's Client class: publish(), publishJSON(), topics, schedules, messages
- Auto-provisioning uses POST /api/v1/public/instant-db
- Events go to POST /v1/zerodb/{project_id}/database/events
- Schedules use hooks API: POST/GET/DELETE/PATCH /v1/zerodb/{project_id}/database/hooks
- Never store credentials in code or tests
- Tests use mocked fetch — no real API calls in CI
