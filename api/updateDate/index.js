const { fetchListRows, updateCustomField } = require('../shared/clickup');
const { isAdmin, sign, parseFieldMap } = require('../shared/utils');

module.exports = async function (context, req) {
  try {
    if (!isAdmin(req)) {
      context.res = { status: 401, body: { error: 'unauthorized' } };
      return;
    }

    const body = req.body || {};
    const sfId = String(body.sf_id || '').trim();
    const metricKey = String(body.metric_key || '').trim();
    const value = String(body.value || '').trim();
    const sig = String(body.sig || '').trim();

    if (!sfId || !metricKey || !sig || sig !== sign(sfId)) {
      context.res = { status: 400, body: { error: 'invalid_payload' } };
      return;
    }

    const rows = await fetchListRows();
    const row = rows.find((r) => r.sf_id === sfId);
    if (!row) {
      context.res = { status: 404, body: { error: 'status_not_found' } };
      return;
    }

    const fieldMap = parseFieldMap();
    const fieldId = fieldMap[metricKey];
    if (!fieldId) {
      context.res = { status: 400, body: { error: 'field_not_mapped' } };
      return;
    }

    const clickupValue = value ? String(new Date(value).getTime()) : null;
    await updateCustomField(row.task_id, fieldId, clickupValue);

    context.res = { status: 200, body: { ok: true } };
  } catch (err) {
    context.log.error(err);
    context.res = { status: 500, body: { error: 'server_error', detail: String(err.message || err) } };
  }
};
