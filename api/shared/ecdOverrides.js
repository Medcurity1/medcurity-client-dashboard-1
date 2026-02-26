let cachedClient = null;

function hasStorageConfig() {
  return Boolean((process.env.ECD_OVERRIDES_STORAGE_CONNECTION_STRING || '').trim());
}

function getTableName() {
  return (process.env.ECD_OVERRIDES_TABLE || 'EcdOverrides').trim();
}

async function getClient() {
  if (!hasStorageConfig()) return null;
  if (cachedClient) return cachedClient;
  const { TableClient } = require('@azure/data-tables');
  const conn = process.env.ECD_OVERRIDES_STORAGE_CONNECTION_STRING;
  const tableName = getTableName();
  const client = TableClient.fromConnectionString(conn, tableName);
  await client.createTable().catch(() => {});
  cachedClient = client;
  return cachedClient;
}

function rowKeyForMetric(metricKey) {
  return Buffer.from(String(metricKey || ''), 'utf8').toString('base64url');
}

async function getOverrides(sfId) {
  const client = await getClient();
  if (!client) return null;
  const out = {};
  const pk = String(sfId || '').trim();
  if (!pk) return out;
  const filter = `PartitionKey eq '${pk.replace(/'/g, "''")}'`;
  for await (const entity of client.listEntities({ queryOptions: { filter } })) {
    const metricKey = String(entity.metricKey || '').trim();
    if (!metricKey) continue;
    const value = entity.value == null ? null : String(entity.value);
    out[metricKey] = value;
  }
  return out;
}

async function replaceOverrides(sfId, overridesMap) {
  const client = await getClient();
  if (!client) return false;
  const pk = String(sfId || '').trim();
  if (!pk) return false;

  const filter = `PartitionKey eq '${pk.replace(/'/g, "''")}'`;
  const existing = [];
  for await (const entity of client.listEntities({ queryOptions: { filter } })) {
    existing.push(entity);
  }
  for (const entity of existing) {
    await client.deleteEntity(entity.partitionKey, entity.rowKey).catch(() => {});
  }

  const src = overridesMap && typeof overridesMap === 'object' ? overridesMap : {};
  const now = new Date().toISOString();
  for (const [metricKey, rawValue] of Object.entries(src)) {
    const mk = String(metricKey || '').trim();
    if (!mk) continue;
    const value = rawValue == null ? null : String(rawValue).trim();
    // Null and empty-string both mean "no override", so skip persisting.
    if (value == null || value === '') continue;
    await client.createEntity({
      partitionKey: pk,
      rowKey: rowKeyForMetric(mk),
      metricKey: mk,
      value,
      updatedAt: now,
    });
  }
  return true;
}

module.exports = {
  hasStorageConfig,
  getOverrides,
  replaceOverrides,
};

