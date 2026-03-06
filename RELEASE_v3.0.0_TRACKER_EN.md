# MongoStudio - Release v3.0.0 Tracker (Updated)

Last updated: 2026-03-07
Status legend: `DONE` | `PARTIAL` | `NOT DONE`

This tracker is aligned with the current codebase and includes newly added features discovered/implemented during the v3.0 hardening cycle.

## 1) Connection & Auth

| Feature | Status | Implemented in |
|---|---|---|
| Advanced Connection Config (TLS) | DONE | `src/components/ConnectPage.jsx`, `server/index.js` connect options |
| `authSource` selector | DONE | `src/components/ConnectPage.jsx` |
| `replicaSet` support | DONE | `src/components/ConnectPage.jsx`, `server/index.js` |
| SRV connection support (`mongodb+srv://`) | DONE | URI parser in `src/components/ConnectPage.jsx` |
| Read Preference selector (5 options) | DONE | `src/components/ConnectPage.jsx` |
| Direct Connection toggle | DONE | `src/components/ConnectPage.jsx`, backend option passthrough |
| Connection URI multiline editor | DONE | `src/components/ConnectPage.jsx` (`showUriEditor`) |
| Connection Profiles (saved connections) | DONE | localStorage profile flow in `src/components/ConnectPage.jsx` |
| Recent Connections list | DONE | `src/components/ConnectPage.jsx` (`RECENT_KEY`) |
| Do-not-save-password option for Recent | DONE | `src/components/ConnectPage.jsx` (`RECENT_PASSWORD_PREF_KEY`) |
| Password encryption at rest for saved profiles | PARTIAL | Password save controls exist, but no encrypted secrets block yet |
| Connection status indicator in top bar | DONE | `src/components/TopBar.jsx` |
| Production Guard badge + warning banner | DONE | `src/components/TopBar.jsx`, `src/components/Workspace.jsx` |
| Confirm dialog for destructive operations | DONE | `src/components/modals/ConfirmDialog.jsx` + DB/collection/doc usage |
| True Read-Only mode (hard server-side mutate blocking) | PARTIAL | Safe mode limits execution, but no dedicated `readOnly` mode contract yet |
| Read Concern selector | NOT DONE | Not exposed in connect UI |
| Write Concern selector | NOT DONE | Not exposed in connect UI |

---

## 2) Execution Safety

| Feature | Status | Implemented in |
|---|---|---|
| Safe / Power mode toggle | DONE | `src/components/TopBar.jsx`, `src/components/SettingsModal.jsx`, `server/index.js` |
| Configurable `maxTimeMS` | DONE | `src/components/SettingsModal.jsx`, execution config endpoint |
| Configurable `maxResultSize` | DONE | `src/components/SettingsModal.jsx`, execution config endpoint |
| Configurable `allowDiskUse` | DONE | `src/components/SettingsModal.jsx`, execution config endpoint |
| Slow/heavy query warning | DONE | `src/components/QueryConsole.jsx` |
| Cancel query | DONE | `AbortController` in `src/components/QueryConsole.jsx` |
| Live execution timer | DONE | `src/components/QueryConsole.jsx` |
| Result size indicator | DONE | `src/components/QueryConsole.jsx` |
| Error classification UX | DONE | `classifyError()` in `server/index.js` + UI surface |
| Global rate-limit controls (service-wide) | DONE | `src/components/SettingsModal.jsx`, `GET/PUT /api/service-config` |
| Persisted backend service config | DONE | `server/service-config.json`, `server/index.js` |
| True streaming results/documents cursor | PARTIAL | Pagination exists; true incremental streaming UX is not complete |

---

## 3) Query & Analysis

| Feature | Status | Implemented in |
|---|---|---|
| Explain integration | DONE | `src/components/QueryConsole.jsx`, explain API path |
| Explain visual summary (scan/index/docs examined) | DONE | `extractExplainSummary()` and summary panel |
| Query History | DONE | localStorage flow in `src/components/QueryConsole.jsx` |
| Query History scoped by `db.collection` | DONE | history context logic in `src/components/QueryConsole.jsx` |
| Query templates menu | DONE | `src/components/QueryConsole.jsx` |
| Query auto-fill suggestions | PARTIAL | Smart suggestions exist, but not full IDE-grade autocomplete |
| Query assist for `db.` / collection helpers | DONE | helper suggestion generation in `src/components/QueryConsole.jsx` |
| Index hint selector | DONE | query hint selector + index list API |
| Export Query results (`JSON/CSV`) | DONE | `src/components/QueryConsole.jsx` export actions |
| Global `Expand all / Collapse all` for Query results | DONE | query result controls + `JsonView` external toggle |
| Query Compare / Diff | NOT DONE | Not implemented |
| Aggregation Visual Builder | NOT DONE | Not implemented |
| Full syntax-aware editor autocomplete engine | NOT DONE | Not implemented |

---

## 4) Data & Collections

| Feature | Status | Implemented in |
|---|---|---|
| Create Database from UI | DONE | `src/components/Sidebar.jsx`, `POST /api/databases` |
| Drop Database from UI | DONE | `src/components/Sidebar.jsx`, `DELETE /api/databases/:db` |
| Create Collection | DONE | `src/components/Sidebar.jsx` |
| Drop Collection | DONE | `src/components/Sidebar.jsx`, backend endpoint |
| Import collection from file/package | DONE | `src/components/Sidebar.jsx`, import endpoint |
| Import documents into collection | DONE | `src/components/Sidebar.jsx`, bulk insert endpoint |
| Import database package | DONE | `src/components/Sidebar.jsx`, `POST /api/databases/import` |
| Export collection (`JSON/CSV`) | DONE | `src/components/CollectionView.jsx`, `src/components/Sidebar.jsx` |
| Export collection with visible fields + sort/filter options | DONE | collection export modal/options flow |
| Export database (package/files, ZIP, schema/index options) | DONE | `src/components/modals/DatabaseExportDialog.jsx`, export APIs |
| Export all databases with explicit DB selection | DONE | sidebar footer action + `DatabaseExportDialog` |
| Index manager (list/create/drop) | DONE | `src/components/IndexesView.jsx`, backend index endpoints |
| Schema analyzer | DONE | `src/components/SchemaView.jsx`, schema endpoint |
| GridFS Viewer | NOT DONE | Not implemented |
| Logical dump/restore with guarded workflow | PARTIAL | Package export/import exists; full guarded pipeline is incomplete |
| Restore flow with mandatory `dry-run -> validate -> confirm -> run` | NOT DONE | Not implemented |

---

## 5) Documents View

| Feature | Status | Implemented in |
|---|---|---|
| JSON/Table view switch | DONE | `src/components/CollectionView.jsx` |
| Pagination controls (`first/prev/next/last`) | DONE | `src/components/CollectionView.jsx` |
| Page size selector | DONE | `src/components/CollectionView.jsx` |
| Page size synchronized with Safe limits | DONE | bound by execution config (`maxResultSize`) |
| Column visibility toggle (Table view) | DONE | `Columns` menu in `src/components/CollectionView.jsx` |
| Field visibility toggle (JSON preview) | DONE | `Fields` mode in the same menu |
| Hidden-items counters | DONE | visible/hidden counters in menu |
| Inline full-value controls | DONE | `Inline` menu in `src/components/CollectionView.jsx` |
| Inline supports objects/arrays/long strings/ObjectId-like values | DONE | inline render logic in `src/components/CollectionView.jsx` |
| Inline edit/insert document | DONE | `src/components/DocumentEditor.jsx` |
| Insert from file | DONE | file insert in `src/components/DocumentEditor.jsx` |
| Sorting in Table view by visible headers | DONE | header sort in `src/components/CollectionView.jsx` |
| JSON view sorting support (`activeSort`) | DONE | sort controls in `src/components/CollectionView.jsx` |
| Global JSON collapse/expand behavior | DONE | `src/components/JsonView.jsx` + global controls |
| True streaming export | PARTIAL | Export works; true chunked stream UX is still limited |

---

## 6) UI / UX / Navigation / Responsive

| Feature | Status | Implemented in |
|---|---|---|
| Dark / Light themes | DONE | theme tokens + toggles |
| Breadcrumb navigation (`db / collection`) | DONE | `src/components/TopBar.jsx` |
| Active DB/collection visual context | DONE | top breadcrumb + sidebar highlight |
| Responsive top bar priority/hiding logic | DONE | adaptive badge/menu behavior in `src/components/TopBar.jsx` |
| Hosts hover dropdown with node list | DONE | `src/components/TopBar.jsx` |
| Session tags shown inside hosts dropdown | DONE | hosts menu session/state sections |
| `Display` tab in Settings | DONE | `src/components/SettingsModal.jsx` |
| `Show tags` setting for top bar | DONE | display settings wiring |
| Top tags hidden by default | DONE | default `showTopTags: false` |
| Sidebar smart default width (longest collection fit) | DONE | width measuring logic in `src/components/Sidebar.jsx` |
| Sidebar manual resize | DONE | drag resize handle in `src/components/Sidebar.jsx` |
| Sidebar dropdown layering/z-index fixes | DONE | z-index + overflow fixes in sidebar/top/query menus |
| Styled controls (`ms-select`, `ms-checkbox`, `ms-range`) | DONE | `src/index.css` + components |
| Stronger hover affordances for clickable controls | DONE | nav/menu/table interactive elements |
| Dismissible hints persisted per session | DONE | connect/home/workspace hint persistence |

---

## 7) Home / Server Info / Observability

| Feature | Status | Implemented in |
|---|---|---|
| Home dashboard cards (DB/collections/docs/size/version/guard) | DONE | `src/components/WelcomePanel.jsx` |
| DB list with progressive loading states | DONE | `src/components/WelcomePanel.jsx` |
| Progressive loading (no fake startup zeros) in key panels | DONE | Home + Sidebar + Settings server info |
| Collapsible info sections (Server Details/Databases/Capabilities) | DONE | `src/components/WelcomePanel.jsx` |
| Audit API | DONE | `GET /api/audit` in `server/index.js` |
| Audit UI page | DONE | `src/components/AuditView.jsx` |
| Audit filters (action/search/range/limit) | DONE | `src/components/AuditView.jsx` |
| Audit user attribution for runtime actions | DONE | `getConnection()` + `auditReq()` in `server/index.js` |
| Health/readiness/metrics endpoints | DONE | `server/index.js` |
| Server Info hosts list as read-only viewer | DONE | `src/components/SettingsModal.jsx` |

---

## 8) New Features Added During v3.0 Iterations

| Feature | Status | Notes |
|---|---|---|
| Export all databases entry in sidebar footer | DONE | one-click flow with modal options |
| Multi-mode DB export (`db package` vs `collection files`) | DONE | unified export dialog |
| Database selection inside "Export all databases" | DONE | includes `Select all` / `Clear all` |
| Collection-level "Import documents" from file | DONE | added to collection context menu |
| Query results export action | DONE | direct export from Query screen |
| Query results global expand/collapse | DONE | all rows toggle |
| Inline behavior tightened for truncated values | DONE | expanded inline rendering behavior |
| Connect-page UX polish (saved/editor sizing, multiline URI edit) | DONE | visual and interaction improvements |
| Service-wide rate-limit management from UI | DONE | persisted backend config |
| Host dropdown behavior kept consistent across viewport states | DONE | top bar menu behavior improvements |

---

## 9) Remaining Backlog (v3.0 Stretch + v3.5)

| Feature | Status | Notes |
|---|---|---|
| Config Import / Export (profiles + app config, encrypted secrets block) | NOT DONE | currently data export/import is implemented, config package is not |
| Connected Clusters Map | NOT DONE | no dedicated cluster map screen yet |
| Cluster Map entry in DB context menu | NOT DONE | dependent on cluster map feature |
| Cluster User & RBAC Manager | NOT DONE | not implemented |
| Cluster management actions | NOT DONE | not implemented |
| Backup snapshot scheduler + retention | NOT DONE | not implemented |
| Multi-user auth/session isolation | NOT DONE | app remains single-user |
| Competitive benchmark track artifacts | NOT DONE | no committed benchmark outputs |

---

## 10) Totals

| Status | Count |
|---|---:|
| DONE | 95 |
| PARTIAL | 6 |
| NOT DONE | 15 |
| **TOTAL** | **116** |

---

## Release Readiness Note

Core v3.0 UX and data workflows are largely implemented. Remaining high-impact gaps before a strict production-ready milestone are:
- true Read-Only mode at backend contract level,
- full streaming flows,
- guarded restore pipeline (`dry-run -> validate -> confirm -> run`),
- full editor-grade autocomplete/diff tooling.
