const { fetchListRows } = require('../shared/clickup');
const { isAdmin, sign } = require('../shared/utils');

module.exports = async function (context, req) {
  try {
    if (!isAdmin(req)) {
      context.res = { status: 401, body: { error: 'unauthorized' } };
      return;
    }

    const rows = await fetchListRows();
    const projects = rows.map((r) => ({
      sf_id: r.sf_id,
      task_name: r.task_name,
      task_status: r.task_status,
      source_updated_at: r.source_updated_at,
      link_sig: sign(r.sf_id),
    }));

    context.res = { status: 200, body: { count: projects.length, projects } };
  } catch (err) {
    context.log.error(err);
    context.res = { status: 500, body: { error: 'server_error', detail: String(err.message || err) } };
  }
};
