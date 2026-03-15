# MongoStudio v2.6.x Manual QA

## Scope

This checklist is the release gate for hardening changes in v2.6.x.
Mode: manual only (no CI e2e runner in this pass).

## Real vs Mock Parity

| Area | Real API | Try Demo (mockApi) | Expected |
|---|---|---|---|
| Connect flow | Yes | Yes (simulated) | Same UI states and error surfaces |
| Documents query/find/pagination | Yes | Yes | Same table/json behavior |
| Operate: insert/update/delete/bulk | Yes | Yes | Same method UI and confirmations |
| Preflight risk check | Yes | Yes (simulated) | Same risk/warning UX |
| Schema quick view | Yes | Yes (simulated) | Same fields/types rendering |
| Export/Import UI flow | Yes | Yes (simulated) | Same controls and result UX |
| Audit filters/export | Yes | Yes (simulated) | Same filter semantics and CSV/JSON export |
| ConsoleUI mode | Yes | Yes | Same parser/help/export/copy UX |
| Real shell modes (`mongo`/`mongosh`) | Yes | No | Not available in mock by design |

## Release Smoke Checklist

1. Container health without Mongo session:
   - `docker ps` reports `healthy`.
   - `GET /api/health` returns `{ status: "ok", ready: true }`.
2. `service-config` access control:
   - no `X-Connection-Id` -> rejected.
   - safe mode connection -> `PUT /api/service-config` returns `403 forbidden`.
   - power mode connection -> `PUT /api/service-config` succeeds.
3. Strict validation:
   - invalid `filter/sort/projection` on documents/explain/export/deleteMany returns `400 validation`.
4. Valid operations still work:
   - normal documents query.
   - valid export JSON/CSV.
   - valid deleteMany with heavy confirm.
5. Preflight audit:
   - operation appears in Audit with action `preflight`.
6. Exact total audit:
   - `exact_total_start` logged.
   - final action logged as `exact_total_done` or `exact_total_timeout` or `exact_total_error`.
7. Status shell probe cache:
   - repeated `GET /api/status` does not trigger probe each request.
   - `GET /api/status?refresh=1` forces refresh.
8. Real shell auth behavior:
   - URI with creds: shell session starts.
   - URI without creds (auth required): clear validation error with hint to use URI creds or ConsoleUI.
9. Toast behavior:
   - close works, timer works, hover slowdown works, no duplicate cleanup regressions.
10. Query history stability:
   - new entries include `id`.
   - old entries still render (fallback key path), no React key warnings.
11. Keyset pagination memory behavior:
   - next/prev page navigation still works.
   - long paging does not grow cursor history without bound.
12. Keyboard accessibility:
   - visible focus ring on connect/query/operate/audit controls with keyboard navigation.
13. Audit search performance:
   - rapid typing does not flood API (debounced).
   - stale responses do not overwrite newer state.
14. Try Demo parity:
   - same UI flows and states as real API for Operate/Query/Schema/Audit.
   - only real shells remain intentionally unavailable.
15. Server Management execution context:
   - selecting an alternate node does not immediately change the applied execution context.
   - confirm/apply flow is required before run-tools execute on another node.
   - managed output path updates only after the new execution context is applied.
16. Server Management monitoring scope:
   - `mongostat`, `mongotop`, and `Slow Ops` support `All nodes`.
   - aggregate monitoring scope remains local to the monitoring panel and does not auto-run destructive actions.
17. Server Management result UX:
   - copy actions work for output path, command preview, stdout, and stderr.
   - long raw output is truncated by default and can be expanded without layout breakage.
18. Server Management inline validation:
   - invalid dump/export query JSON is surfaced before `Run`.
   - CSV export requires explicit fields and blocks invalid submission in the UI.
