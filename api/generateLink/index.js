const { isAdmin, sign } = require('../shared/utils');

module.exports = async function (context, req) {
  try {
    if (!isAdmin(req)) {
      context.res = { status: 401, body: { error: 'unauthorized' } };
      return;
    }

    const sfId = String(req.query.sf_id || req.query.sfId || '').trim();
    if (!sfId) {
      context.res = { status: 400, body: { error: 'missing_sf_id' } };
      return;
    }

    context.res = {
      status: 200,
      body: {
        sf_id: sfId,
        signature: sign(sfId),
        relative_status_url: `/status.html?sf_id=${encodeURIComponent(sfId)}&sig=${sign(sfId)}`,
      },
    };
  } catch (err) {
    context.log.error(err);
    context.res = { status: 500, body: { error: 'server_error', detail: String(err.message || err) } };
  }
};
