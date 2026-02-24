# Medcurity Client Dashboard 1 (Azure Static Web App)

This version is rewritten for Azure Static Web Apps:
- Frontend in `app/`
- API in `api/` (Azure Functions)

## Routes
- `/admin/projects?key=...`
- `/admin/metrics?key=...`
- `/status?sf_id=...&sig=...`

## Required Environment Variables (Static Web App > Configuration)
- `CLICKUP_API_TOKEN`
- `CLICKUP_LIST_ID`
- `CLICKUP_SF_ID_FIELD_ID`
- `CLIENT_LINK_SECRET`
- `ADMIN_API_KEY`
- `CLICKUP_FIELD_MAP_JSON`

## Azure Static Web App Build Settings
- App location: `app`
- Api location: `api`
- Output location: *(leave blank)*

Redeploy trigger: 2026-02-24T23:00:33Z
