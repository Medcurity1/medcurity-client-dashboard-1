let poolPromise = null;
let schemaReady = false;

function hasSqlConfig() {
  return Boolean((process.env.AZURE_SQL_CONNECTION_STRING || '').trim());
}

async function getPool() {
  if (!hasSqlConfig()) return null;
  if (!poolPromise) {
    const sql = require('mssql');
    poolPromise = sql.connect(process.env.AZURE_SQL_CONNECTION_STRING);
  }
  const pool = await poolPromise;
  if (!schemaReady) {
    await ensureSchema(pool);
    schemaReady = true;
  }
  return pool;
}

async function ensureSchema(pool) {
  const sqlText = `
IF OBJECT_ID(N'dbo.ecd_overrides', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.ecd_overrides (
    sf_id NVARCHAR(100) NOT NULL,
    metric_key NVARCHAR(200) NOT NULL,
    value NVARCHAR(50) NULL,
    updated_at DATETIME2 NOT NULL CONSTRAINT DF_ecd_overrides_updated_at DEFAULT SYSUTCDATETIME(),
    updated_by NVARCHAR(200) NULL,
    CONSTRAINT PK_ecd_overrides PRIMARY KEY (sf_id, metric_key)
  );
END;

IF OBJECT_ID(N'dbo.audit_events', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.audit_events (
    id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    sf_id NVARCHAR(100) NOT NULL,
    task_id NVARCHAR(100) NULL,
    event_type NVARCHAR(50) NOT NULL,
    metric_key NVARCHAR(200) NULL,
    old_value NVARCHAR(MAX) NULL,
    new_value NVARCHAR(MAX) NULL,
    actor NVARCHAR(200) NULL,
    metadata_json NVARCHAR(MAX) NULL,
    created_at DATETIME2 NOT NULL CONSTRAINT DF_audit_events_created_at DEFAULT SYSUTCDATETIME()
  );

  CREATE INDEX IX_audit_events_sf_id_created_at ON dbo.audit_events (sf_id, created_at DESC);
END;

IF OBJECT_ID(N'dbo.client_links', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.client_links (
    sf_id NVARCHAR(100) NOT NULL PRIMARY KEY,
    sig NVARCHAR(128) NOT NULL,
    client_url NVARCHAR(500) NOT NULL,
    last_generated_at DATETIME2 NOT NULL CONSTRAINT DF_client_links_generated_at DEFAULT SYSUTCDATETIME()
  );
END;
`;
  await pool.request().batch(sqlText);
}

function cleanOverridesMap(source) {
  const src = source && typeof source === 'object' ? source : {};
  const out = {};
  for (const [metricKey, rawValue] of Object.entries(src)) {
    const mk = String(metricKey || '').trim();
    if (!mk) continue;
    if (rawValue == null) continue;
    const value = String(rawValue).trim();
    if (!value) continue;
    out[mk] = value;
  }
  return out;
}

async function getOverrides(sfId) {
  const pool = await getPool();
  if (!pool) return null;
  const key = String(sfId || '').trim();
  if (!key) return {};
  const result = await pool
    .request()
    .input('sf_id', key)
    .query(`
      SELECT metric_key, value
      FROM dbo.ecd_overrides
      WHERE sf_id = @sf_id
    `);
  const out = {};
  for (const row of result.recordset || []) {
    const metricKey = String(row.metric_key || '').trim();
    if (!metricKey) continue;
    out[metricKey] = row.value == null ? null : String(row.value);
  }
  return out;
}

async function replaceOverrides(sfId, overridesMap, actor = null) {
  const pool = await getPool();
  if (!pool) return false;
  const key = String(sfId || '').trim();
  if (!key) return false;
  const cleaned = cleanOverridesMap(overridesMap);
  const tx = pool.transaction();
  await tx.begin();
  try {
    await tx.request().input('sf_id', key).query('DELETE FROM dbo.ecd_overrides WHERE sf_id = @sf_id;');
    for (const [metricKey, value] of Object.entries(cleaned)) {
      await tx.request()
        .input('sf_id', key)
        .input('metric_key', metricKey)
        .input('value', value)
        .input('updated_by', actor || null)
        .query(`
          INSERT INTO dbo.ecd_overrides (sf_id, metric_key, value, updated_by)
          VALUES (@sf_id, @metric_key, @value, @updated_by);
        `);
    }
    await tx.commit();
    return true;
  } catch (err) {
    await tx.rollback().catch(() => {});
    throw err;
  }
}

async function recordAuditEvent({ sfId, taskId = null, eventType, metricKey = null, oldValue = null, newValue = null, actor = null, metadata = null }) {
  const pool = await getPool();
  if (!pool) return false;
  const sf = String(sfId || '').trim();
  const event = String(eventType || '').trim();
  if (!sf || !event) return false;
  await pool.request()
    .input('sf_id', sf)
    .input('task_id', taskId ? String(taskId) : null)
    .input('event_type', event)
    .input('metric_key', metricKey ? String(metricKey) : null)
    .input('old_value', oldValue == null ? null : String(oldValue))
    .input('new_value', newValue == null ? null : String(newValue))
    .input('actor', actor ? String(actor) : null)
    .input('metadata_json', metadata ? JSON.stringify(metadata) : null)
    .query(`
      INSERT INTO dbo.audit_events (sf_id, task_id, event_type, metric_key, old_value, new_value, actor, metadata_json)
      VALUES (@sf_id, @task_id, @event_type, @metric_key, @old_value, @new_value, @actor, @metadata_json);
    `);
  return true;
}

async function upsertClientLink({ sfId, signature, clientUrl }) {
  const pool = await getPool();
  if (!pool) return false;
  const sf = String(sfId || '').trim();
  if (!sf) return false;
  await pool.request()
    .input('sf_id', sf)
    .input('sig', String(signature || '').trim())
    .input('client_url', String(clientUrl || '').trim())
    .query(`
      MERGE dbo.client_links AS target
      USING (SELECT @sf_id AS sf_id) AS src
      ON target.sf_id = src.sf_id
      WHEN MATCHED THEN
        UPDATE SET sig = @sig, client_url = @client_url, last_generated_at = SYSUTCDATETIME()
      WHEN NOT MATCHED THEN
        INSERT (sf_id, sig, client_url) VALUES (@sf_id, @sig, @client_url);
    `);
  return true;
}

module.exports = {
  hasSqlConfig,
  getOverrides,
  replaceOverrides,
  recordAuditEvent,
  upsertClientLink,
};

