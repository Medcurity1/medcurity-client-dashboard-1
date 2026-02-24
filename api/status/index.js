const { fetchListRows } = require('../shared/clickup');
const { sign } = require('../shared/utils');

module.exports = async function (context, req) {
  try {
    const sfId = String(context.bindingData.sfId || '').trim();
    const sig = String(req.query.sig || '').trim();
    if (!sfId || sig !== sign(sfId)) {
      context.res = { status: 403, body: { error: 'forbidden' } };
      return;
    }

    const rows = await fetchListRows();
    const row = rows.find((r) => r.sf_id === sfId);
    if (!row) {
      context.res = { status: 404, body: { error: 'not_found' } };
      return;
    }

    context.res = { status: 200, body: row };
  } catch (err) {
    context.log.error(err);
    context.res = { status: 500, body: { error: 'server_error', detail: String(err.message || err) } };
  }
};
