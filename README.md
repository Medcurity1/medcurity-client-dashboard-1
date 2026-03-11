# Medcurity Client Dashboard 1 (Azure Static Web App)

This version is rewritten for Azure Static Web Apps:
- Frontend in `app/`
- API in `api/` (Azure Functions)

## Routes
- `/admin/projects?key=...`
- `/admin/metrics?key=...`
- `/assessor?token=...` (read-only lead-filtered internal view)
- `/assessor?lead=...&sig=...` (read-only signed assessor link; no new env var needed)
- `/status?sf_id=...&sig=...`
- `/api/generateLink?sf_id=...&key=...` (returns signed client URL)
- `/api/assessorProjects?token=...`
- `/api/generateAssessorLink?lead=...&key=...`
- `/api/storageHealth?key=...` (confirms Azure SQL connectivity and table counts)

## Required Environment Variables (Static Web App > Configuration)
- `CLICKUP_API_TOKEN`
- `CLICKUP_LIST_ID`
- `CLICKUP_SF_ID_FIELD_ID`
- `CLIENT_LINK_SECRET`
- `ADMIN_API_KEY`
- `CLICKUP_FIELD_MAP_JSON`
- `AZURE_SQL_CONNECTION_STRING`
- `ASSESSOR_ACCESS_JSON` (token->lead mapping for internal read-only assessor dashboard)

## Recommended Environment Variables
- `CLIENT_PUBLIC_BASE_URL=https://status.medcurity.com`
- `ADMIN_ALLOWED_HOSTS=staging.status.medcurity.com,localhost,127.0.0.1`
- `STAGING_ADMIN_HOST=staging.status.medcurity.com`
- `ADMIN_BYPASS_KEY_ON_STAGING=true`

## Azure SQL Backend
The API now uses Azure SQL for shared dashboard persistence:
- ECD overrides
- audit event history
- generated client link tracking

Schema bootstrap script:
- `api/sql/bootstrap.sql`

The app also auto-creates these tables on first SQL connection:
- `dbo.ecd_overrides`
- `dbo.audit_events`
- `dbo.client_links`

If `AZURE_SQL_CONNECTION_STRING` is unset:
- hosted environments are read-only for ECD override persistence
- local (`localhost`) can still use browser-local fallback for development only

## Azure Static Web App Build Settings
- App location: `app`
- Api location: `api`
- Output location: *(leave blank)*

Redeploy trigger: 2026-02-24T23:00:33Z


Example assessor mapping:
`ASSESSOR_ACCESS_JSON={"token_for_jordan":{"assessor_name":"Jordan","lead_values":["Jordan"]},"token_for_amanda":{"assessor_name":"Amanda","lead_values":["Amanda"]}}`
