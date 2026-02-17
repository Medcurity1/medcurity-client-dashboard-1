# Medcurity Client Dashboard

Client-specific project status dashboard powered by ClickUp task data.

Each client gets a unique URL based on Salesforce ID (`SF ID`) with a signed token so clients can only view their own page.

## What this app does

- Pulls ClickUp tasks from a list
- Uses a custom field (`SF ID`) as the client identifier
- Maps selected ClickUp custom fields into Project Details, SRA Track, and NVA Track sections
- Stores latest snapshot in SQLite
- Supports near-real-time updates from a ClickUp webhook

## Project structure

- `app.py` - Flask app and routes
- `clickup_service.py` - ClickUp API calls and normalization
- `storage.py` - SQLite persistence
- `config.py` - environment loading and config parsing
- `templates/status.html` - client dashboard UI
- `static/styles.css` - styling

## 1) Setup

1. Create and activate a virtual environment
2. Install dependencies:

```bash
python3 -m pip install -r requirements.txt
```

3. Create `.env` from template:

```bash
cp .env.template .env
```

4. Fill in `.env` values:
- `CLICKUP_API_TOKEN`
- `CLICKUP_LIST_ID`
- `CLICKUP_SF_ID_FIELD_ID`
- `CLIENT_LINK_SECRET`
- `CLICKUP_FIELD_MAP_JSON`

### Field key format for `CLICKUP_FIELD_MAP_JSON`

Use these key patterns to place values in the UI:

- `project.<field>` for top Project Details values
- `project.sra_enabled` as SRA checkbox toggle (true/false)
- `project.nva_enabled` as NVA checkbox toggle (true/false)
- `sra.<step_name>.<field>` for SRA track cards
- `nva.<step_name>.<field>` for NVA track cards
- Any other key appears under Additional Metrics

Example:

```json
{
  "project.sra_enabled": "cf_sra_enabled_checkbox",
  "project.nva_enabled": "cf_nva_enabled_checkbox",
  "project.ecd": "cf_project_ecd",
  "project.acd": "cf_project_acd",
  "project.project_lead": "cf_project_lead",
  "project.project_support": "cf_project_support",
  "project.time_zone": "cf_timezone",
  "project.location": "cf_location",
  "sra.sra_kickoff.status": "cf_sra_kickoff_status",
  "sra.sra_kickoff.owner": "cf_sra_kickoff_owner",
  "sra.sra_kickoff.ecd": "cf_sra_kickoff_ecd",
  "sra.sra_kickoff.acd": "cf_sra_kickoff_acd",
  "nva.verify_access.status": "cf_nva_verify_access_status",
  "nva.verify_access.owner": "cf_nva_verify_access_owner"
}
```

## 2) Run

```bash
python3 app.py
```

Server starts on `http://localhost:8080` by default.

## 3) Initial data sync

Run a full sync from ClickUp:

```bash
curl -X POST http://localhost:8080/admin/sync
```

If `ADMIN_API_KEY` is set, add header:

```bash
curl -X POST http://localhost:8080/admin/sync -H "X-API-Key: <ADMIN_API_KEY>"
```

## 4) Generate client URL

```bash
curl http://localhost:8080/admin/generate-link/<SF_ID>
```

Response includes:
- `signature`
- `url_path` (share this full URL with the client)

## 5) Webhook updates from ClickUp

Create a ClickUp webhook that targets:

- without token: `https://your-domain/webhook/clickup`
- with token: `https://your-domain/webhook/clickup?token=<WEBHOOK_TOKEN>`

When task updates fire, the app fetches the latest task by `task_id` and refreshes that client's dashboard row.

## Notes

- If multiple tasks have the same `SF ID`, the latest synced task wins.
- You can change dashboard metrics by editing only `CLICKUP_FIELD_MAP_JSON`.
- For production, put this behind HTTPS and set `ADMIN_API_KEY`.

## Azure deployment (share with clients)

This project is ready for Azure App Service deployment from GitHub.

### A) Create the web app

1. In Azure Portal, create a **Web App**.
2. Publish: **Code**
3. Runtime stack: **Python 3.12** (or latest supported Python 3.x)
4. Region: pick closest to your users.
5. Pricing:
- Azure default domain (`*.azurewebsites.net`) is included.
- Custom domain usually requires a paid App Service plan tier.

### B) Connect GitHub repo

1. In Web App -> **Deployment Center**
2. Source: **GitHub**
3. Repository: `Jackfrost830/medcurity-client-dashboard`
4. Branch: `main`
5. Save and deploy

### C) Configure startup command

In Web App -> **Configuration** -> **General settings** -> Startup Command:

```bash
gunicorn --bind=0.0.0.0 --timeout 120 app:app
```

### D) Configure environment variables

In Web App -> **Configuration** -> **Application settings**, add:

- `CLICKUP_API_TOKEN`
- `CLICKUP_LIST_ID`
- `CLICKUP_SF_ID_FIELD_ID`
- `CLIENT_LINK_SECRET`
- `CLICKUP_FIELD_MAP_JSON`
- `ADMIN_API_KEY`
- `WEBHOOK_TOKEN`
- `DATABASE_PATH` = `/home/site/wwwroot/client_status.db`

Then save and restart.

### E) Initial sync and link generation

After deploy, run:

```bash
curl -X POST https://<your-app>.azurewebsites.net/admin/sync -H "X-API-Key: <ADMIN_API_KEY>"
```

Then generate client links:

```bash
curl https://<your-app>.azurewebsites.net/admin/generate-link/<SF_ID> -H "X-API-Key: <ADMIN_API_KEY>"
```

### F) ClickUp webhook

Set ClickUp webhook URL:

`https://<your-app>.azurewebsites.net/webhook/clickup?token=<WEBHOOK_TOKEN>`

### G) Custom domain

After app works on `azurewebsites.net`, add your domain in:
Web App -> **Custom domains**.
