const {
  required,
  parseFieldMap,
  toDateUS,
  normalizeText,
} = require('./utils');
const {
  hasSqlConfig,
  getCachedClickupRows,
  replaceCachedClickupRows,
} = require('./dashboardStore');

const API_BASE = 'https://api.clickup.com/api/v2';
const LIST_CACHE_TTL_MS = 60 * 1000;
const SQL_CACHE_REFRESH_MS = 10 * 60 * 1000;
const COMMENT_CACHE_TTL_MS = 2 * 60 * 1000;

let listCacheRows = null;
let listCacheUntil = 0;
let listCacheInFlight = null;
const commentCache = new Map();
const commentInFlight = new Map();

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

  // Fallback: auto-detect month/year style planning fields when not mapped explicitly.
  // This keeps assessor work-month filtering working even if CLICKUP_FIELD_MAP_JSON
  // does not include these fields yet.
  const customFields = Array.isArray(task.custom_fields) ? task.custom_fields : [];
  const monthField = customFields.find((f) => {
    const n = normalizeText(f?.name || '');
    return n.includes('month year') || n.includes('primary work month') || n === 'work month';
  });
  if (monthField) {
    const monthValue = customFieldDisplayValue(monthField);
    if (monthValue) {
      if (!String(metrics['project.month_year'] || '').trim()) metrics['project.month_year'] = monthValue;
      if (!String(metrics['project.primary_work_month'] || '').trim()) metrics['project.primary_work_month'] = monthValue;
    }
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

function preserveNextStepsFromCached(rows, cachedRows) {
  const current = Array.isArray(rows) ? rows : [];
  const prior = Array.isArray(cachedRows) ? cachedRows : [];
  if (!current.length || !prior.length) return;
  const bySf = new Map(prior.map((r) => [String(r?.sf_id || ''), r]));
  for (const row of current) {
    const sf = String(row?.sf_id || '');
    if (!sf) continue;
    const nextSteps = String(row?.metrics?.['project.next_steps'] || '').trim();
    if (nextSteps) continue;
    const cached = bySf.get(sf);
    const cachedNextSteps = String(cached?.metrics?.['project.next_steps'] || '').trim();
    if (!cachedNextSteps) continue;
    row.metrics = row.metrics || {};
    row.metrics['project.next_steps'] = cachedNextSteps;
  }
}

async function fetchListRows(options = {}) {
  const force = !!options.force;
  const includeComments = !!options.includeComments;
  const now = Date.now();
  if (!force && listCacheRows && now < listCacheUntil) return listCacheRows;
  if (!force && listCacheInFlight) return listCacheInFlight;

  if (!force && hasSqlConfig()) {
    try {
      const cached = await getCachedClickupRows();
      if (cached && Array.isArray(cached.rows) && cached.rows.length) {
        listCacheRows = cached.rows;
        listCacheUntil = Date.now() + LIST_CACHE_TTL_MS;
        const stale = !cached.latestSyncMs || (Date.now() - cached.latestSyncMs) > SQL_CACHE_REFRESH_MS;
        if (stale && !listCacheInFlight) {
          const refresh = (async () => {
            try {
              await fetchListRows({ force: true });
            } catch (_) {
              // Keep serving cached SQL snapshot if refresh fails.
            }
          })();
          listCacheInFlight = refresh;
          refresh.finally(() => {
            if (listCacheInFlight === refresh) listCacheInFlight = null;
          });
        }
        return listCacheRows;
      }
    } catch (_) {
      // Fall through to live ClickUp fetch.
    }
  }

  const run = (async () => {
  const listId = required('CLICKUP_LIST_ID');
  const all = [];
  let cachedRowsForMerge = null;
  if (!includeComments && hasSqlConfig()) {
    try {
      const cached = await getCachedClickupRows();
      cachedRowsForMerge = cached && Array.isArray(cached.rows) ? cached.rows : null;
    } catch (_) {
      cachedRowsForMerge = null;
    }
  }
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
    const rows = pickBestBySf(all);
    if (includeComments) {
      await hydrateNextStepsFromComments(rows);
    } else if (cachedRowsForMerge) {
      preserveNextStepsFromCached(rows, cachedRowsForMerge);
    }
    listCacheRows = rows;
    listCacheUntil = Date.now() + LIST_CACHE_TTL_MS;
    if (hasSqlConfig()) {
      replaceCachedClickupRows(rows).catch(() => {});
    }
    return rows;
  })();

  listCacheInFlight = run;
  try {
    return await run;
  } finally {
    listCacheInFlight = null;
  }
}

async function hydrateNextStepsFromComments(rows) {
  const targets = (Array.isArray(rows) ? rows : []).filter((row) => {
    const current = String(row?.metrics?.['project.next_steps'] || '').trim();
    return !current && String(row?.task_id || '').trim();
  });
  if (!targets.length) return;

  const concurrency = 4;
  let idx = 0;
  async function worker() {
    while (idx < targets.length) {
      const currentIdx = idx;
      idx += 1;
      const row = targets[currentIdx];
      try {
        const latestComment = await fetchLatestTaskComment(row.task_id);
        if (latestComment) {
          row.metrics = row.metrics || {};
          row.metrics['project.next_steps'] = latestComment;
        }
      } catch (_) {
        // Ignore per-task failures; keep refresh running for other rows.
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, targets.length) }, () => worker()));
}

async function updateCustomField(taskId, fieldId, value) {
  return fetchJson(`/task/${taskId}/field/${fieldId}`, {
    method: 'POST',
    body: JSON.stringify({ value }),
  });
}

async function fetchLatestTaskComment(taskId) {
  const key = String(taskId || '').trim();
  if (!key) return '';
  const cached = commentCache.get(key);
  const now = Date.now();
  if (cached && now < cached.until) return cached.value;
  if (commentInFlight.has(key)) return commentInFlight.get(key);

  const run = (async () => {
  const data = await fetchJson(`/task/${taskId}/comment`);
  const comments = Array.isArray(data.comments) ? data.comments : [];
    if (!comments.length) {
      commentCache.set(key, { value: '', until: Date.now() + COMMENT_CACHE_TTL_MS });
      return '';
    }

  comments.sort((a, b) => Number(b.date || 0) - Number(a.date || 0));
  const latest = comments[0] || {};
    const text = String(latest.comment_text || '').trim();
    commentCache.set(key, { value: text, until: Date.now() + COMMENT_CACHE_TTL_MS });
    return text;
  })();

  commentInFlight.set(key, run);
  try {
    return await run;
  } finally {
    commentInFlight.delete(key);
  }
}

module.exports = {
  fetchListRows,
  updateCustomField,
  fetchLatestTaskComment,
};
