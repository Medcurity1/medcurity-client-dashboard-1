const { app } = require('@azure/functions');
const { fetchListRows, updateCustomField } = require('../shared/clickup');
const { isAdmin, sign, parseUSDate, dateDiffBusinessDays, quarterLabel, parseFieldMap } = require('../shared/utils');

function json(status, body) {
  return {
    status,
    jsonBody: body,
    headers: { 'Content-Type': 'application/json' },
  };
}

function getMetric(metrics, ...keys) {
  for (const key of keys) {
    const value = String(metrics[key] || '').trim();
    if (value) return value;
  }
  return '';
}

app.http('status', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'status',
  handler: async (req, ctx) => {
    try {
      const sfId = String(req.query.get('sf_id') || req.query.get('sfId') || '').trim();
      const sig = String(req.query.get('sig') || '').trim();
      if (!sfId || sig !== sign(sfId)) return json(403, { error: 'forbidden' });

      const rows = await fetchListRows();
      const row = rows.find((r) => r.sf_id === sfId);
      if (!row) return json(404, { error: 'not_found' });
      return json(200, row);
    } catch (err) {
      ctx.error(err);
      return json(500, { error: 'server_error', detail: String(err.message || err) });
    }
  },
});

app.http('projects', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'projects',
  handler: async (req, ctx) => {
    try {
      if (!isAdmin({ query: Object.fromEntries(req.query.entries()), headers: req.headers })) {
        return json(401, { error: 'unauthorized' });
      }
      const rows = await fetchListRows();
      const projects = rows.map((r) => ({
        sf_id: r.sf_id,
        task_name: r.task_name,
        task_status: r.task_status,
        source_updated_at: r.source_updated_at,
        link_sig: sign(r.sf_id),
      }));
      return json(200, { count: projects.length, projects });
    } catch (err) {
      ctx.error(err);
      return json(500, { error: 'server_error', detail: String(err.message || err) });
    }
  },
});

app.http('metrics', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'metrics',
  handler: async (req, ctx) => {
    try {
      if (!isAdmin({ query: Object.fromEntries(req.query.entries()), headers: req.headers })) {
        return json(401, { error: 'unauthorized' });
      }
      const rows = await fetchListRows();
      const completed = rows.filter((r) => String(r.task_status || '').toLowerCase() === 'completed');
      const items = [];
      for (const r of completed) {
        const m = r.metrics || {};
        const sraStart = parseUSDate(getMetric(m, 'sra.sra_kickoff.date', 'sra.sra_kickoff.acd'));
        const sraEnd = parseUSDate(getMetric(m, 'sra.present_final_sra_report.date', 'sra.present_final_sra_report.acd'));
        const sraDays = dateDiffBusinessDays(sraStart, sraEnd);
        if (sraDays) items.push({ track: 'SRA', days: sraDays, quarter: quarterLabel(sraEnd) });

        const nvaStart = parseUSDate(getMetric(m, 'nva.nva_kickoff.date', 'nva.nva_kickoff.acd'));
        const nvaEnd = parseUSDate(getMetric(m, 'nva.present_final_nva_report.date', 'nva.present_final_nva_report.acd'));
        const nvaDays = dateDiffBusinessDays(nvaStart, nvaEnd);
        if (nvaDays) items.push({ track: 'NVA', days: nvaDays, quarter: quarterLabel(nvaEnd) });
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
      return json(200, { total_tracked_closures: items.length, avg_close_days: avg, quarters });
    } catch (err) {
      ctx.error(err);
      return json(500, { error: 'server_error', detail: String(err.message || err) });
    }
  },
});

app.http('generateLink', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'generateLink',
  handler: async (req, ctx) => {
    try {
      if (!isAdmin({ query: Object.fromEntries(req.query.entries()), headers: req.headers })) {
        return json(401, { error: 'unauthorized' });
      }
      const sfId = String(req.query.get('sf_id') || req.query.get('sfId') || '').trim();
      if (!sfId) return json(400, { error: 'missing_sf_id' });
      const signature = sign(sfId);
      return json(200, {
        sf_id: sfId,
        signature,
        relative_status_url: `/status?sf_id=${encodeURIComponent(sfId)}&sig=${signature}`,
      });
    } catch (err) {
      ctx.error(err);
      return json(500, { error: 'server_error', detail: String(err.message || err) });
    }
  },
});

app.http('update', {
  methods: ['POST'],
  authLevel: 'anonymous',
  route: 'update',
  handler: async (req, ctx) => {
    try {
      const queryObj = Object.fromEntries(req.query.entries());
      if (!isAdmin({ query: queryObj, headers: req.headers })) return json(401, { error: 'unauthorized' });

      const body = (await req.json()) || {};
      const sfId = String(body.sf_id || '').trim();
      const metricKey = String(body.metric_key || '').trim();
      const value = String(body.value || '').trim();
      const sig = String(body.sig || '').trim();
      if (!sfId || !metricKey || !sig || sig !== sign(sfId)) return json(400, { error: 'invalid_payload' });

      const rows = await fetchListRows();
      const row = rows.find((r) => r.sf_id === sfId);
      if (!row) return json(404, { error: 'status_not_found' });

      const fieldMap = parseFieldMap();
      const fieldId = fieldMap[metricKey];
      if (!fieldId) return json(400, { error: 'field_not_mapped' });

      const clickupValue = value ? String(new Date(value).getTime()) : null;
      await updateCustomField(row.task_id, fieldId, clickupValue);
      return json(200, { ok: true });
    } catch (err) {
      ctx.error(err);
      return json(500, { error: 'server_error', detail: String(err.message || err) });
    }
  },
});
