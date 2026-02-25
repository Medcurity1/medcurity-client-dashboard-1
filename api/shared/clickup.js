const {
  required,
  parseFieldMap,
  toDateUS,
  normalizeText,
} = require('./utils');

const API_BASE = 'https://api.clickup.com/api/v2';

async function fetchJson(path, init = {}) {
  const token = required('CLICKUP_API_TOKEN');
  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: {
      Authorization: token,
      'Content-Type': 'application/json',
      ...(init.headers || {}),
    },
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`ClickUp ${response.status}: ${text.slice(0, 500)}`);
  }
  return response.json();
}

function getCustomField(task, fieldId) {
  return (task.custom_fields || []).find((f) => String(f.id) === String(fieldId));
}

function customFieldDisplayValue(field) {
  if (!field || field.value == null) return '';
  const type = String(field.type || '').toLowerCase();
  const value = field.value;

  if (type === 'date') return toDateUS(value);
  if (type === 'checkbox') return value ? 'true' : 'false';
  if (type === 'users' && Array.isArray(value)) {
    return value.map((u) => u.username || u.email || u.initials || '').filter(Boolean).join(', ');
  }
  if ((type === 'drop_down' || type === 'labels') && field.type_config && Array.isArray(field.type_config.options)) {
    const vals = Array.isArray(value) ? value : [value];
    const byOrder = new Map(field.type_config.options.map((o) => [String(o.orderindex), o.name]));
    return vals.map((v) => byOrder.get(String(v))).filter(Boolean).join(', ');
  }
  return String(value);
}

function normalizeTask(task) {
  const fieldMap = parseFieldMap();
  const sfIdField = required('CLICKUP_SF_ID_FIELD_ID');
  const sfField = getCustomField(task, sfIdField);
  const sfId = customFieldDisplayValue(sfField).trim();
  if (!sfId) return null;

  const metrics = {};
  for (const [metricKey, fieldId] of Object.entries(fieldMap)) {
    metrics[metricKey] = customFieldDisplayValue(getCustomField(task, fieldId));
  }

  return {
    sf_id: sfId,
    task_id: String(task.id || ''),
    task_name: String(task.name || ''),
    task_url: String(task.url || ''),
    task_status: String(task.status?.status || ''),
    task_status_type: normalizeText(task.status?.type || ''),
    task_created_at: toDateUS(task.date_created),
    task_closed_at: toDateUS(task.date_closed),
    source_updated_at: toDateUS(task.date_updated),
    metrics,
  };
}

function pickBestBySf(rows) {
  const bySf = new Map();
  for (const row of rows) {
    const cur = bySf.get(row.sf_id);
    if (!cur) {
      bySf.set(row.sf_id, row);
      continue;
    }
    const curClosed = cur.task_status_type === 'closed';
    const rowClosed = row.task_status_type === 'closed';
    if (curClosed && !rowClosed) {
      bySf.set(row.sf_id, row);
      continue;
    }
    const curTime = Date.parse(cur.source_updated_at || '') || 0;
    const rowTime = Date.parse(row.source_updated_at || '') || 0;
    if (rowTime >= curTime) bySf.set(row.sf_id, row);
  }
  return Array.from(bySf.values());
}

async function fetchListRows() {
  const listId = required('CLICKUP_LIST_ID');
  const all = [];
  let page = 0;
  while (true) {
    const data = await fetchJson(`/list/${listId}/task?include_closed=true&page=${page}`);
    const tasks = Array.isArray(data.tasks) ? data.tasks : [];
    for (const t of tasks) {
      const row = normalizeTask(t);
      if (row) all.push(row);
    }
    if (tasks.length < 100) break;
    page += 1;
  }
  return pickBestBySf(all);
}

async function updateCustomField(taskId, fieldId, value) {
  return fetchJson(`/task/${taskId}/field/${fieldId}`, {
    method: 'POST',
    body: JSON.stringify({ value }),
  });
}

async function fetchLatestTaskComment(taskId) {
  const data = await fetchJson(`/task/${taskId}/comment`);
  const comments = Array.isArray(data.comments) ? data.comments : [];
  if (!comments.length) return '';

  comments.sort((a, b) => Number(b.date || 0) - Number(a.date || 0));
  const latest = comments[0] || {};
  return String(latest.comment_text || '').trim();
}

module.exports = {
  fetchListRows,
  updateCustomField,
  fetchLatestTaskComment,
};
