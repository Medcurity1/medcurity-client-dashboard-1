const { fetchListRows } = require('../shared/clickup');
const { isAdmin, parseUSDate, dateDiffBusinessDays, quarterLabel } = require('../shared/utils');

function getMetric(metrics, ...keys) {
  for (const key of keys) {
    const value = String(metrics[key] || '').trim();
    if (value) return value;
  }
  return '';
}

module.exports = async function (context, req) {
  try {
    if (!isAdmin(req)) {
      context.res = { status: 401, body: { error: 'unauthorized' } };
      return;
    }

    const rows = await fetchListRows();
    const completed = rows.filter((r) => String(r.task_status || '').toLowerCase() === 'completed');

    const items = [];
    for (const r of completed) {
      const m = r.metrics || {};

      const sraStart = parseUSDate(getMetric(m, 'sra.sra_kickoff.date', 'sra.sra_kickoff.acd'));
      const sraEnd = parseUSDate(getMetric(m, 'sra.present_final_sra_report.date', 'sra.present_final_sra_report.acd'));
      const sraDays = dateDiffBusinessDays(sraStart, sraEnd);
      if (sraDays) {
        items.push({ track: 'SRA', days: sraDays, quarter: quarterLabel(sraEnd), company: r.task_name });
      }

      const nvaStart = parseUSDate(getMetric(m, 'nva.nva_kickoff.date', 'nva.nva_kickoff.acd'));
      const nvaEnd = parseUSDate(getMetric(m, 'nva.present_final_nva_report.date', 'nva.present_final_nva_report.acd'));
      const nvaDays = dateDiffBusinessDays(nvaStart, nvaEnd);
      if (nvaDays) {
        items.push({ track: 'NVA', days: nvaDays, quarter: quarterLabel(nvaEnd), company: r.task_name });
      }
    }

    const sum = items.reduce((a, i) => a + i.days, 0);
    const avg = items.length ? Math.round((sum / items.length) * 10) / 10 : 0;

    const byQuarter = {};
    for (const i of items) {
      byQuarter[i.quarter] = byQuarter[i.quarter] || { count: 0, sum: 0 };
      byQuarter[i.quarter].count += 1;
      byQuarter[i.quarter].sum += i.days;
    }

    const quarters = Object.entries(byQuarter)
      .map(([quarter, v]) => ({ quarter, count: v.count, avg_close_days: Math.round((v.sum / v.count) * 10) / 10 }))
      .sort((a, b) => a.quarter.localeCompare(b.quarter));

    context.res = {
      status: 200,
      body: {
        total_tracked_closures: items.length,
        avg_close_days: avg,
        quarters,
      },
    };
  } catch (err) {
    context.log.error(err);
    context.res = { status: 500, body: { error: 'server_error', detail: String(err.message || err) } };
  }
};
