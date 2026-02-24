const { app } = require('@azure/functions');
const { fetchListRows, updateCustomField } = require('../shared/clickup');
const { isAdmin, sign, parseUSDate, dateDiffBusinessDays, quarterLabel, parseFieldMap } = require('../shared/utils');

function json(status, body) {
  return { status, jsonBody: body, headers: { 'Content-Type': 'application/json' } };
}

function getMetric(metrics, ...keys) {
  for (const key of keys) {
    const value = String(metrics[key] || '').trim();
    if (value) return value;
  }
  return '';
}

function titleCase(value) {
  return String(value || '')
    .replace(/[_-]/g, ' ')
    .split(' ')
    .filter(Boolean)
    .map((s) => s.charAt(0).toUpperCase() + s.slice(1).toLowerCase())
    .join(' ')
    .replace(/Sra/g, 'SRA')
    .replace(/Nva/g, 'NVA')
    .replace(/Baa/g, 'BAA')
    .replace(/Acd/g, 'ACD')
    .replace(/Ecd/g, 'ECD');
}

function parseBool(v) {
  const t = String(v || '').trim().toLowerCase();
  return t === 'true' || t === '1' || t === 'yes';
}

function statusClass(status) {
  const s = String(status || 'Not Started');
  if (s === 'On Track' || s === 'Completed') return 'status-pill-green';
  if (s === 'Potential Roadblock') return 'status-pill-yellow';
  if (s === 'Roadblock/Overage') return 'status-pill-red';
  return 'status-pill-neutral';
}

function computeStepStatus(step) {
  if (step.ACD) return step.isKickoff ? 'Completed' : 'On Track';
  if (step.isKickoff) return 'Not Started';
  return step.ECD ? 'On Track' : 'Not Started';
}

function stepDisplayName(section, slug, location) {
  const loc = String(location || '').toLowerCase();
  const onsite = loc.includes('onsite');
  const remote = loc.includes('remote');
  if (slug === 'schedule_onsite_remote_interview') {
    if (onsite && !remote) return 'Schedule Onsite Visit';
    if (remote && !onsite) return 'Schedule Interview Sessions';
    return 'Schedule Onsite/Remote Interview';
  }
  if (slug === 'go_onsite_have_interview') {
    if (onsite && !remote) return 'Go Onsite/Have Interviews';
    if (remote && !onsite) return 'Conduct Interview Sessions';
    return 'Go Onsite/Have Interviews';
  }
  const labels = {
    sra_kickoff: 'SRA Kickoff',
    receive_policies_and_procedures_baa: 'Receive Policies and Procedures / BAA',
    review_policies_and_procedures_baa: 'Review Policies and Procedures / BAA',
    recieve_requested_follow_up_documentation: 'Recieve Requested Follow up Documentation',
    review_sra: 'Review SRA',
    schedule_final_sra_report: 'Schedule Final SRA Report',
    present_final_sra_report: 'Present Final SRA Report',
    nva_kickoff: 'NVA Kickoff',
    receive_credentials: 'Receive Credentials',
    verify_access: 'Verify Access',
    scans_complete: 'Scans Complete',
    access_removed: 'Access Removed',
    compile_report: 'Compile Report',
    schedule_final_nva_report: 'Schedule Final NVA Report',
    present_final_nva_report: 'Present Final NVA Report',
  };
  return labels[slug] || titleCase(slug);
}

function stepOwner(section, slug, clientName) {
  const medcurityOnly = new Set([
    'sra_kickoff',
    'go_onsite_have_interview',
    'review_sra',
    'present_final_sra_report',
    'nva_kickoff',
    'scans_complete',
    'access_removed',
    'present_final_nva_report',
  ]);
  const shared = new Set([
    'schedule_onsite_remote_interview',
    'receive_policies_and_procedures_baa',
    'review_policies_and_procedures_baa',
  ]);
  if (medcurityOnly.has(slug)) return 'Medcurity';
  if (shared.has(slug)) return `Medcurity & ${clientName}`;
  return clientName || 'Not assigned';
}

function buildDashboard(row) {
  const metrics = row.metrics || {};
  const location = getMetric(metrics, 'project.remote_onsite', 'project.location');
  const showSra = parseBool(getMetric(metrics, 'project.sra_enabled'));
  const showNva = parseBool(getMetric(metrics, 'project.nva_enabled'));

  const projectDetails = {
    Status: titleCase(row.task_status),
    'Project Lead': getMetric(metrics, 'project.project_lead') || 'Not assigned',
    Location: location || 'Not set',
    'Project Support': getMetric(metrics, 'project.project_support') || '',
    'Next Steps': getMetric(metrics, 'project.next_steps') || 'Not set',
  };
  if (!projectDetails['Project Support']) delete projectDetails['Project Support'];

  const sraSteps = {};
  const nvaSteps = {};

  for (const [key, value] of Object.entries(metrics)) {
    const parts = key.split('.');
    if (parts.length < 3) continue;
    const section = parts[0].toLowerCase();
    if (section !== 'sra' && section !== 'nva') continue;
    const slug = parts[1].toLowerCase();
    const field = parts[2].toLowerCase();
    const stepName = stepDisplayName(section, slug, location);
    const target = section === 'sra' ? sraSteps : nvaSteps;
    target[stepName] = target[stepName] || {
      step_slug: slug,
      Status: 'Not Started',
      Owner: stepOwner(section, slug, row.task_name),
      ECD: '',
      ACD: '',
      ecd: { editable: false, metric_key: `${section}.${slug}.ecd`, value: '', input_value: '' },
      acd: { editable: true, metric_key: `${section}.${slug}.date`, value: '', input_value: '' },
      extras: [],
      status_class: 'status-pill-neutral',
      status: 'Not Started',
    };

    if (field === 'date' || field === 'acd') {
      target[stepName].ACD = String(value || '').trim();
    } else if (field === 'ecd') {
      target[stepName].ECD = String(value || '').trim();
    } else {
      target[stepName].extras.push({ label: titleCase(field), value: String(value || '').trim() || 'Not set' });
    }
  }

  function normalizeSteps(stepMap) {
    Object.values(stepMap).forEach((step) => {
      step.isKickoff = step.step_slug.includes('kickoff');
      if (!step.ECD && step.isKickoff && step.ACD) step.ECD = step.ACD;
      step.status = computeStepStatus(step);
      step.status_class = statusClass(step.status);
      step.ecd.value = step.ECD || 'Not set';
      step.acd.value = step.ACD || 'Not set';
      step.ecd.input_value = '';
      step.acd.input_value = '';
    });
    return stepMap;
  }

  return {
    project_details: projectDetails,
    show_sra: showSra,
    show_nva: showNva,
    sra_steps: normalizeSteps(sraSteps),
    nva_steps: normalizeSteps(nvaSteps),
    extra_metrics: {},
  };
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
      return json(200, { ...row, dashboard: buildDashboard(row) });
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
        task_created_at: r.task_created_at,
        task_closed_at: r.task_closed_at,
        project_lead: getMetric(r.metrics || {}, 'project.project_lead') || 'Not assigned',
        project_support: getMetric(r.metrics || {}, 'project.project_support') || '',
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
        if (sraDays) items.push({ company: r.task_name, track: 'SRA', days: sraDays, quarter: quarterLabel(sraEnd) });

        const nvaStart = parseUSDate(getMetric(m, 'nva.nva_kickoff.date', 'nva.nva_kickoff.acd'));
        const nvaEnd = parseUSDate(getMetric(m, 'nva.present_final_nva_report.date', 'nva.present_final_nva_report.acd'));
        const nvaDays = dateDiffBusinessDays(nvaStart, nvaEnd);
        if (nvaDays) items.push({ company: r.task_name, track: 'NVA', days: nvaDays, quarter: quarterLabel(nvaEnd) });
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
      return json(200, { total_tracked_closures: items.length, avg_close_days: avg, quarters, rows: items });
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
      return json(200, { sf_id: sfId, signature, relative_status_url: `/status?sf_id=${encodeURIComponent(sfId)}&sig=${signature}` });
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
