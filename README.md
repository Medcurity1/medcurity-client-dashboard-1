# Medcurity Client Dashboard 1 (Azure Static Web App)

This version is rewritten for Azure Static Web Apps:
- Frontend in `app/`
- API in `api/` (Azure Functions)

## Routes
- `/admin/projects?key=...`
- `/admin/metrics?key=...`
- `/status?sf_id=...&sig=...`
- `/api/generateLink?sf_id=...&key=...` (returns signed client URL)

## Required Environment Variables (Static Web App > Configuration)
- `CLICKUP_API_TOKEN`
- `CLICKUP_LIST_ID`
- `CLICKUP_SF_ID_FIELD_ID`
- `CLIENT_LINK_SECRET`
- `ADMIN_API_KEY`
- `CLICKUP_FIELD_MAP_JSON`

## Recommended Environment Variables
- `CLIENT_PUBLIC_BASE_URL=https://status.medcurity.com`
- `ADMIN_ALLOWED_HOSTS=staging.status.medcurity.com,localhost,127.0.0.1`

## Shared ECD Override Backend (for live updates across all users)
If you want ECD overrides to sync for everyone (not browser-local only), set:
- `ECD_OVERRIDES_STORAGE_CONNECTION_STRING=<Azure Storage connection string>`
- `ECD_OVERRIDES_TABLE=EcdOverrides` (optional; default is `EcdOverrides`)

When unset, ECD overrides fall back to browser local storage only.

## Azure Static Web App Build Settings
- App location: `app`
- Api location: `api`
- Output location: *(leave blank)*

Redeploy trigger: 2026-02-24T23:00:33Z
