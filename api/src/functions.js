const { app } = require('@azure/functions');
const { fetchListRows, updateCustomField } = require('../shared/clickup');
const { isAdmin, sign, parseUSDate, dateDiffBusinessDays, quarterLabel, parseFieldMap } = require('../shared/utils');
const fs = require('fs');
const path = require('path');

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
  if (!step.ECD) return 'Not Started';

  const ecd = parseUSDate(step.ECD);
  if (!ecd) return 'Not Started';
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const ecdDate = new Date(ecd.getFullYear(), ecd.getMonth(), ecd.getDate());
  const diffMs = ecdDate.getTime() - today.getTime();
  const diffDays = Math.ceil(diffMs / 86400000);
  if (diffDays < 0) return 'Roadblock/Overage';
  if (diffDays <= 3) return 'Potential Roadblock';
  return 'On Track';
}

function toInputDate(dateUS) {
  const m = String(dateUS || '').trim().match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (!m) return '';
  return `${m[3]}-${m[1]}-${m[2]}`;
}

function shiftToMondayIfWeekend(d) {
  const out = new Date(d.getFullYear(), d.getMonth(), d.getDate());
  const wd = out.getDay();
  if (wd === 6) out.setDate(out.getDate() + 2);
  if (wd === 0) out.setDate(out.getDate() + 1);
  return out;
}

function shiftToFridayIfWeekend(d) {
  const out = new Date(d.getFullYear(), d.getMonth(), d.getDate());
  const wd = out.getDay();
  if (wd === 6) out.setDate(out.getDate() - 1);
  if (wd === 0) out.setDate(out.getDate() - 2);
  return out;
}

function nextBusinessDay(d) {
  const out = new Date(d.getFullYear(), d.getMonth(), d.getDate());
  out.setDate(out.getDate() + 1);
  return shiftToMondayIfWeekend(out);
}

function addDays(d, days) {
  const out = new Date(d.getFullYear(), d.getMonth(), d.getDate());
  out.setDate(out.getDate() + days);
  return out;
}

function parseMetricDate(v) {
  const dt = parseAnyUSDate(v);
  if (!dt) return null;
  return new Date(dt.getFullYear(), dt.getMonth(), dt.getDate());
}

function findStepNameBySlug(stepMap, slug) {
  return Object.keys(stepMap).find((name) => String(stepMap[name]?.step_slug || '') === slug) || null;
}

function anchorDateForSlug(stepMap, slug) {
  const name = findStepNameBySlug(stepMap, slug);
  if (!name) return null;
  const step = stepMap[name] || {};
  return parseMetricDate(step.ACD) || parseMetricDate(step.ECD);
}

function setEcdFromDateIfBlank(stepMap, slug, anchorDate, days) {
  const name = findStepNameBySlug(stepMap, slug);
  if (!name || !anchorDate) return;
  const step = stepMap[name];
  if (String(step.ECD || '').trim()) return;
  const candidate = shiftToMondayIfWeekend(addDays(anchorDate, days));
  step.ECD = formatUSDate(candidate);
}

function setEcdIfBlank(stepMap, slug, anchorSlug, days) {
  const anchor = anchorDateForSlug(stepMap, anchorSlug);
  setEcdFromDateIfBlank(stepMap, slug, anchor, days);
}

function shiftBusinessSafe(value, deltaDays) {
  const shifted = addDays(value, deltaDays);
  if (deltaDays < 0) return shiftToFridayIfWeekend(shifted);
  return shiftToMondayIfWeekend(shifted);
}

function addEcdAcdFields(stepMap, offsets) {
  const kickoffName = Object.keys(stepMap).find((n) => String(stepMap[n]?.step_slug || '').includes('kickoff')) || null;
  const kickoffSlug = kickoffName ? String(stepMap[kickoffName]?.step_slug || '') : '';
  if (kickoffName) {
    const kickoff = stepMap[kickoffName];
    kickoff.ECD = kickoff.ACD || '';
    kickoff.ecd.editable = false;
  }

  // SRA explicit rules
  setEcdIfBlank(stepMap, 'receive_policies_and_procedures_baa', 'sra_kickoff', 7);
  setEcdIfBlank(stepMap, 'review_policies_and_procedures_baa', 'receive_policies_and_procedures_baa', 12);
  setEcdIfBlank(stepMap, 'schedule_onsite_remote_interview', 'sra_kickoff', 14);

  const goName = findStepNameBySlug(stepMap, 'go_onsite_have_interview');
  if (goName) {
    const goStep = stepMap[goName];
    const goAcd = parseMetricDate(goStep.ACD);
    if (goAcd) goStep.ECD = formatUSDate(goAcd);
    else setEcdIfBlank(stepMap, 'go_onsite_have_interview', 'review_policies_and_procedures_baa', 7);
  }

  setEcdIfBlank(stepMap, 'recieve_requested_follow_up_documentation', 'go_onsite_have_interview', 14);
  setEcdIfBlank(stepMap, 'schedule_final_sra_report', 'go_onsite_have_interview', 14);

  const reviewName = findStepNameBySlug(stepMap, 'review_sra');
  if (reviewName) {
    const review = stepMap[reviewName];
    if (!String(review.ECD || '').trim()) {
      const presentName = findStepNameBySlug(stepMap, 'present_final_sra_report');
      const presentAcd = presentName ? parseMetricDate(stepMap[presentName]?.ACD) : null;
      if (presentAcd) {
        review.ECD = formatUSDate(shiftToFridayIfWeekend(addDays(presentAcd, -1)));
      } else {
        const goAnchor = anchorDateForSlug(stepMap, 'go_onsite_have_interview');
        if (goAnchor) {
          let proposed = shiftToMondayIfWeekend(addDays(goAnchor, 15));
          const siblings = [];
          const receiveName = findStepNameBySlug(stepMap, 'recieve_requested_follow_up_documentation');
          const scheduleName = findStepNameBySlug(stepMap, 'schedule_final_sra_report');
          if (receiveName) {
            const d = parseMetricDate(stepMap[receiveName]?.ECD);
            if (d) siblings.push(d);
          }
          if (scheduleName) {
            const d = parseMetricDate(stepMap[scheduleName]?.ECD);
            if (d) siblings.push(d);
          }
          if (siblings.length) {
            const latest = siblings.reduce((a, b) => (a > b ? a : b));
            if (proposed <= latest) proposed = nextBusinessDay(latest);
          }
          review.ECD = formatUSDate(proposed);
        }
      }
    }
  }

  const presentSraName = findStepNameBySlug(stepMap, 'present_final_sra_report');
  if (presentSraName) {
    const present = stepMap[presentSraName];
    if (!String(present.ECD || '').trim()) {
      const presentAcd = parseMetricDate(present.ACD);
      if (presentAcd) present.ECD = formatUSDate(presentAcd);
      else {
        const reviewTitle = findStepNameBySlug(stepMap, 'review_sra');
        const reviewEcd = reviewTitle ? parseMetricDate(stepMap[reviewTitle]?.ECD) : null;
        const reviewAcd = reviewTitle ? parseMetricDate(stepMap[reviewTitle]?.ACD) : null;
        let reviewAnchor = reviewEcd;
        if (reviewAcd && (!reviewEcd || reviewAcd > reviewEcd)) reviewAnchor = reviewAcd;
        if (reviewAnchor) {
          let candidate = shiftToMondayIfWeekend(addDays(reviewAnchor, 7));
          const prereqSlugs = ['recieve_requested_follow_up_documentation', 'schedule_final_sra_report', 'review_sra'];
          const prereqDates = prereqSlugs.map((s) => anchorDateForSlug(stepMap, s)).filter(Boolean);
          if (prereqDates.length) {
            const minAllowed = prereqDates.reduce((a, b) => (a > b ? a : b));
            if (candidate <= minAllowed) candidate = nextBusinessDay(minAllowed);
          }
          present.ECD = formatUSDate(candidate);
        }
      }
    }
  }

  // NVA explicit rules
  setEcdIfBlank(stepMap, 'receive_credentials', 'nva_kickoff', 7);
  setEcdIfBlank(stepMap, 'verify_access', 'receive_credentials', 7);

  const scansName = findStepNameBySlug(stepMap, 'scans_complete');
  if (scansName) {
    const scans = stepMap[scansName];
    if (!String(scans.ECD || '').trim()) {
      const receiveName = findStepNameBySlug(stepMap, 'receive_credentials');
      const verifyName = findStepNameBySlug(stepMap, 'verify_access');
      const receiveAcd = receiveName ? parseMetricDate(stepMap[receiveName]?.ACD) : null;
      const verifyAcd = verifyName ? parseMetricDate(stepMap[verifyName]?.ACD) : null;
      if (!receiveAcd || !verifyAcd) {
        const kickoffAnchor = anchorDateForSlug(stepMap, 'nva_kickoff');
        setEcdFromDateIfBlank(stepMap, 'scans_complete', kickoffAnchor, 28);
      } else {
        const maxAcd = receiveAcd > verifyAcd ? receiveAcd : verifyAcd;
        setEcdFromDateIfBlank(stepMap, 'scans_complete', maxAcd, 21);
      }
    }
  }

  const scansEcd = parseMetricDate(stepMap[scansName]?.ECD);
  const scansAcd = parseMetricDate(stepMap[scansName]?.ACD);
  const presentNvaName = findStepNameBySlug(stepMap, 'present_final_nva_report');
  const presentNvaAcd = presentNvaName ? parseMetricDate(stepMap[presentNvaName]?.ACD) : null;

  const compileName = findStepNameBySlug(stepMap, 'compile_report');
  if (compileName) {
    if (presentNvaAcd) stepMap[compileName].ECD = formatUSDate(shiftToFridayIfWeekend(addDays(presentNvaAcd, -1)));
    else setEcdFromDateIfBlank(stepMap, 'compile_report', scansEcd, 7);
  }

  const accessName = findStepNameBySlug(stepMap, 'access_removed');
  if (accessName) {
    if (presentNvaAcd) stepMap[accessName].ECD = formatUSDate(shiftToFridayIfWeekend(addDays(presentNvaAcd, -1)));
    else setEcdFromDateIfBlank(stepMap, 'access_removed', scansEcd, 5);
  }

  const scheduleNvaName = findStepNameBySlug(stepMap, 'schedule_final_nva_report');
  if (scheduleNvaName) {
    if (scansAcd) setEcdFromDateIfBlank(stepMap, 'schedule_final_nva_report', scansAcd, 21);
    else setEcdFromDateIfBlank(stepMap, 'schedule_final_nva_report', scansEcd, 12);
  }

  if (presentNvaName) {
    if (presentNvaAcd) stepMap[presentNvaName].ECD = formatUSDate(presentNvaAcd);
    else setEcdFromDateIfBlank(stepMap, 'present_final_nva_report', scansEcd, 19);
  }

  // Fallback offset rules.
  const explicit = new Set([
    'receive_policies_and_procedures_baa',
    'review_policies_and_procedures_baa',
    'schedule_onsite_remote_interview',
    'go_onsite_have_interview',
    'recieve_requested_follow_up_documentation',
    'review_sra',
    'schedule_final_sra_report',
    'present_final_sra_report',
    'sra_kickoff',
    'receive_credentials',
    'verify_access',
    'scans_complete',
    'access_removed',
    'compile_report',
    'schedule_final_nva_report',
    'present_final_nva_report',
    'nva_kickoff',
  ]);
  for (const [slug, offset] of Object.entries(offsets || {})) {
    if (explicit.has(slug)) continue;
    if (kickoffSlug) setEcdIfBlank(stepMap, slug, kickoffSlug, offset);
  }

  // Status + data bindings
  for (const step of Object.values(stepMap)) {
    step.isKickoff = String(step.step_slug || '').includes('kickoff');
    step.status = computeStepStatus(step);
    step.status_class = statusClass(step.status);
    step.ecd.value = step.ECD || 'Not set';
    step.acd.value = step.ACD || 'Not set';
    step.ecd.input_value = toInputDate(step.ECD);
    step.acd.input_value = toInputDate(step.ACD);
    if (!step.isKickoff) step.ecd.editable = true;
  }
}

function parseAnyUSDate(value) {
  const text = String(value || '').trim();
  if (!text) return null;
  const m = text.match(/(\d{1,2})\/(\d{1,2})\/(\d{2,4})/);
  if (!m) return null;
  let year = Number(m[3]);
  if (year < 100) year += year >= 70 ? 1900 : 2000;
  const month = Number(m[1]);
  const day = Number(m[2]);
  const dt = new Date(year, month - 1, day);
  return Number.isNaN(dt.getTime()) ? null : dt;
}

function formatUSDate(d) {
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const yyyy = d.getFullYear();
  return `${mm}/${dd}/${yyyy}`;
}

function quarterFromDate(d) {
  const q = Math.floor(d.getMonth() / 3) + 1;
  return `${d.getFullYear()} Q${q}`;
}

function normalizeCompanyName(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/\(remote\)/g, '')
    .replace(/\(renewal\)/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function parseCsvRows(text) {
  const rows = [];
  let row = [];
  let cell = '';
  let inQuotes = false;

  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    if (ch === '"') {
      if (inQuotes && text[i + 1] === '"') {
        cell += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === ',' && !inQuotes) {
      row.push(cell);
      cell = '';
    } else if ((ch === '\n' || ch === '\r') && !inQuotes) {
      if (ch === '\r' && text[i + 1] === '\n') i += 1;
      row.push(cell);
      cell = '';
      rows.push(row);
      row = [];
    } else {
      cell += ch;
    }
  }
  if (cell.length || row.length) {
    row.push(cell);
    rows.push(row);
  }
  return rows;
}

function normalizeHeader(h) {
  return String(h || '')
    .replace(/\r/g, ' ')
    .replace(/\n/g, ' ')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ');
}

let historicalCache = null;
let historicalCacheMtime = 0;
function loadHistoricalRows() {
  const csvPath = path.join(__dirname, '..', 'data', 'closed-projects.csv');
  if (!fs.existsSync(csvPath)) return [];
  const stat = fs.statSync(csvPath);
  if (historicalCache && historicalCacheMtime === stat.mtimeMs) return historicalCache;

  const raw = fs.readFileSync(csvPath, 'utf8');
  const csvRows = parseCsvRows(raw);
  if (!csvRows.length) return [];

  let headerIdx = -1;
  for (let i = 0; i < csvRows.length; i += 1) {
    const normalized = csvRows[i].map(normalizeHeader);
    if (normalized.includes('company') && normalized.includes('total days')) {
      headerIdx = i;
      break;
    }
  }
  if (headerIdx < 0) return [];

  const headers = csvRows[headerIdx];
  const keyToIdx = new Map();
  headers.forEach((h, idx) => keyToIdx.set(normalizeHeader(h), idx));
  const idxCompany = keyToIdx.get('company');
  const idxSfId = keyToIdx.get('sf id');
  const idxTotalDays = keyToIdx.get('total days');
  const idxSraKickoff = keyToIdx.get('sra kickoff (sra)');
  const idxSraFinal = keyToIdx.get('present final sra report (sra)');
  const idxNvaKickoff = keyToIdx.get('nva kickoff (nva)');
  const idxNvaFinal = keyToIdx.get('present final nva report (nva)');
  if (idxCompany == null || idxTotalDays == null) return [];

  const out = [];
  let currentQuarter = '';
  for (let i = headerIdx + 1; i < csvRows.length; i += 1) {
    const row = csvRows[i];
    const company = String(row[idxCompany] || '').trim();
    if (!company) continue;
    const qMatch = company.match(/^Q([1-4])\s+(\d{4})$/i);
    if (qMatch) {
      currentQuarter = `${qMatch[2]} Q${qMatch[1]}`;
      continue;
    }
    const companyLower = company.toLowerCase();
    if (companyLower.startsWith('totals') || companyLower.startsWith('202')) continue;

    const totalDaysText = String(row[idxTotalDays] || '').replace(/,/g, '');
    const daysMatch = totalDaysText.match(/-?\d+/);
    if (!daysMatch) continue;
    const totalDays = Number(daysMatch[0]);
    if (!Number.isFinite(totalDays) || totalDays <= 0) continue;

    const sraKickoff = parseAnyUSDate(idxSraKickoff != null ? row[idxSraKickoff] : '');
    const sraFinal = parseAnyUSDate(idxSraFinal != null ? row[idxSraFinal] : '');
    const nvaKickoff = parseAnyUSDate(idxNvaKickoff != null ? row[idxNvaKickoff] : '');
    const nvaFinal = parseAnyUSDate(idxNvaFinal != null ? row[idxNvaFinal] : '');
    const final = sraFinal || nvaFinal;
    if (!final) continue;

    let track = 'Unknown';
    if (sraFinal && nvaFinal) track = 'SRA+NVA';
    else if (sraFinal) track = 'SRA';
    else if (nvaFinal) track = 'NVA';

    out.push({
      source_key: `hist|${company}|${track}|${formatUSDate(final)}|${totalDays}`,
      sf_id: idxSfId != null ? String(row[idxSfId] || '').trim() : '',
      company,
      track,
      kickoff_date: sraKickoff ? formatUSDate(sraKickoff) : nvaKickoff ? formatUSDate(nvaKickoff) : '',
      final_date: formatUSDate(final),
      close_days: totalDays,
      quarter_label: currentQuarter || quarterFromDate(final),
      source: 'historical_csv',
    });
  }

  historicalCache = out;
  historicalCacheMtime = stat.mtimeMs;
  return out;
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
  const sraOrder = [
    'sra_kickoff',
    'receive_policies_and_procedures_baa',
    'review_policies_and_procedures_baa',
    'schedule_onsite_remote_interview',
    'go_onsite_have_interview',
    'recieve_requested_follow_up_documentation',
    'review_sra',
    'schedule_final_sra_report',
    'present_final_sra_report',
  ];
  const nvaOrder = [
    'nva_kickoff',
    'receive_credentials',
    'verify_access',
    'scans_complete',
    'access_removed',
    'compile_report',
    'schedule_final_nva_report',
    'present_final_nva_report',
  ];

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
      ecd: { editable: true, metric_key: `${section}.${slug}.ecd`, value: '', input_value: '' },
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

  function normalizeSteps(stepMap, offsets) {
    addEcdAcdFields(stepMap, offsets);
    return stepMap;
  }

  function orderSteps(stepMap, order) {
    const entries = Object.entries(stepMap);
    entries.sort((a, b) => {
      const ai = order.indexOf(a[1].step_slug);
      const bi = order.indexOf(b[1].step_slug);
      if (ai === -1 && bi === -1) return a[0].localeCompare(b[0]);
      if (ai === -1) return 1;
      if (bi === -1) return -1;
      return ai - bi;
    });
    return Object.fromEntries(entries);
  }

  if (showSra && showNva) {
    const sraKickoff = findStepNameBySlug(sraSteps, 'sra_kickoff');
    const nvaKickoff = findStepNameBySlug(nvaSteps, 'nva_kickoff');
    if (sraKickoff && nvaKickoff) {
      const sraAcd = String(sraSteps[sraKickoff]?.ACD || '').trim();
      if (sraAcd) nvaSteps[nvaKickoff].ACD = sraAcd;
    }
  }

  const normalizedSra = normalizeSteps(sraSteps, {
    receive_policies_and_procedures_baa: 7,
    review_policies_and_procedures_baa: 19,
    schedule_onsite_remote_interview: 14,
    go_onsite_have_interview: 21,
    recieve_requested_follow_up_documentation: 28,
    review_sra: 35,
    schedule_final_sra_report: 42,
    present_final_sra_report: 49,
  });

  const normalizedNva = normalizeSteps(nvaSteps, {
    receive_credentials: 7,
    verify_access: 14,
    scans_complete: 21,
    access_removed: 28,
    compile_report: 35,
    schedule_final_nva_report: 42,
    present_final_nva_report: 49,
  });

  // Keep Present Final NVA ECD aligned with Present Final SRA ECD.
  const sraPresentName = findStepNameBySlug(normalizedSra, 'present_final_sra_report');
  const nvaPresentName = findStepNameBySlug(normalizedNva, 'present_final_nva_report');
  if (sraPresentName && nvaPresentName) {
    const sraEcd = String(normalizedSra[sraPresentName]?.ECD || '').trim();
    if (sraEcd) {
      normalizedNva[nvaPresentName].ECD = sraEcd;
      normalizedNva[nvaPresentName].ecd.value = sraEcd;
      normalizedNva[nvaPresentName].ecd.input_value = toInputDate(sraEcd);
      normalizedNva[nvaPresentName].status = computeStepStatus(normalizedNva[nvaPresentName]);
      normalizedNva[nvaPresentName].status_class = statusClass(normalizedNva[nvaPresentName].status);
    }
  }

  return {
    project_details: projectDetails,
    show_sra: showSra,
    show_nva: showNva,
    sra_steps: orderSteps(normalizedSra, sraOrder),
    nva_steps: orderSteps(normalizedNva, nvaOrder),
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
        sra_final_date: getMetric(r.metrics || {}, 'sra.present_final_sra_report.date', 'sra.present_final_sra_report.acd'),
        nva_final_date: getMetric(r.metrics || {}, 'nva.present_final_nva_report.date', 'nva.present_final_nva_report.acd'),
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
      const TRACKED_MAX_CLOSE_DAYS = 120;
      const completed = rows.filter((r) => String(r.task_status || '').toLowerCase() === 'completed');
      const liveItems = [];
      const missingRecords = [];
      let missingSraDates = 0;
      let missingNvaDates = 0;
      let completedWithValidClose = 0;

      for (const r of completed) {
        const m = r.metrics || {};
        let hasValid = false;

        const sraStart = parseAnyUSDate(getMetric(m, 'sra.sra_kickoff.date', 'sra.sra_kickoff.acd'));
        const sraEnd = parseAnyUSDate(getMetric(m, 'sra.present_final_sra_report.date', 'sra.present_final_sra_report.acd'));
        const sraDays = dateDiffBusinessDays(sraStart, sraEnd);
        const sraEnabled = parseBool(getMetric(m, 'project.sra_enabled', 'sra.enabled', 'sra_enabled'));
        const sraRelevant = sraEnabled === true || Object.keys(m).some((k) => String(k).startsWith('sra.'));
        if (sraDays) {
          hasValid = true;
          liveItems.push({
            sf_id: r.sf_id || '',
            company: r.task_name,
            track: 'SRA',
            kickoff_date: sraStart ? formatUSDate(sraStart) : '',
            final_date: sraEnd ? formatUSDate(sraEnd) : '',
            close_days: sraDays,
            quarter_label: quarterLabel(sraEnd),
            source: 'clickup_live',
          });
        } else if (sraRelevant) {
          missingSraDates += 1;
        }

        const nvaStart = parseAnyUSDate(getMetric(m, 'nva.nva_kickoff.date', 'nva.nva_kickoff.acd'));
        const nvaEnd = parseAnyUSDate(getMetric(m, 'nva.present_final_nva_report.date', 'nva.present_final_nva_report.acd'));
        const nvaDays = dateDiffBusinessDays(nvaStart, nvaEnd);
        const nvaEnabled = parseBool(getMetric(m, 'project.nva_enabled', 'nva.enabled', 'nva_enabled'));
        const nvaRelevant = nvaEnabled === true || Object.keys(m).some((k) => String(k).startsWith('nva.'));
        if (nvaDays) {
          hasValid = true;
          liveItems.push({
            sf_id: r.sf_id || '',
            company: r.task_name,
            track: 'NVA',
            kickoff_date: nvaStart ? formatUSDate(nvaStart) : '',
            final_date: nvaEnd ? formatUSDate(nvaEnd) : '',
            close_days: nvaDays,
            quarter_label: quarterLabel(nvaEnd),
            source: 'clickup_live',
          });
        } else if (nvaRelevant) {
          missingNvaDates += 1;
        }

        if (hasValid) completedWithValidClose += 1;
        else missingRecords.push({ company: r.task_name || '', sf_id: r.sf_id || '' });
      }

      const historicalItems = loadHistoricalRows();
      const historicalKeys = new Set(
        historicalItems.map((r) => [
          normalizeCompanyName(r.company),
          String(r.track || '').toUpperCase(),
          String(r.kickoff_date || '').trim(),
          String(r.final_date || '').trim(),
        ].join('|'))
      );
      const liveDeduped = liveItems.filter((r) => {
        const key = [
          normalizeCompanyName(r.company),
          String(r.track || '').toUpperCase(),
          String(r.kickoff_date || '').trim(),
          String(r.final_date || '').trim(),
        ].join('|');
        return !historicalKeys.has(key);
      });

      const allRows = [...historicalItems, ...liveDeduped];
      allRows.sort((a, b) => {
        const ad = parseAnyUSDate(a.final_date);
        const bd = parseAnyUSDate(b.final_date);
        const at = ad ? ad.getTime() : 0;
        const bt = bd ? bd.getTime() : 0;
        return bt - at;
      });

      const allDays = allRows.map((r) => Number(r.close_days)).filter((n) => Number.isFinite(n) && n > 0);
      const sraRows = allRows.filter((r) => String(r.track || '').toUpperCase().includes('SRA'));
      const nvaRows = allRows.filter((r) => String(r.track || '').toUpperCase().includes('NVA'));
      const sraDays = sraRows.map((r) => Number(r.close_days)).filter((n) => Number.isFinite(n) && n > 0);
      const nvaDays = nvaRows.map((r) => Number(r.close_days)).filter((n) => Number.isFinite(n) && n > 0);
      const trackedDays = allDays.filter((d) => d <= TRACKED_MAX_CLOSE_DAYS);
      const avg = allDays.length ? Math.round((allDays.reduce((a, b) => a + b, 0) / allDays.length) * 10) / 10 : null;
      const sraAvg = sraDays.length ? Math.round((sraDays.reduce((a, b) => a + b, 0) / sraDays.length) * 10) / 10 : null;
      const nvaAvg = nvaDays.length ? Math.round((nvaDays.reduce((a, b) => a + b, 0) / nvaDays.length) * 10) / 10 : null;

      const grouped = new Map();
      for (const row of allRows) {
        const q = String(row.quarter_label || '');
        if (!grouped.has(q)) grouped.set(q, []);
        grouped.get(q).push(Number(row.close_days));
      }
      const groupedSra = new Map();
      for (const row of sraRows) {
        const q = String(row.quarter_label || '');
        if (!groupedSra.has(q)) groupedSra.set(q, []);
        groupedSra.get(q).push(Number(row.close_days));
      }
      const groupedNva = new Map();
      for (const row of nvaRows) {
        const q = String(row.quarter_label || '');
        if (!groupedNva.has(q)) groupedNva.set(q, []);
        groupedNva.get(q).push(Number(row.close_days));
      }
      const quarterRows = Array.from(grouped.keys())
        .sort((a, b) => {
          const pa = String(a).match(/^(\d{4}) Q([1-4])$/);
          const pb = String(b).match(/^(\d{4}) Q([1-4])$/);
          if (!pa && !pb) return String(a).localeCompare(String(b));
          if (!pa) return -1;
          if (!pb) return 1;
          if (pa[1] !== pb[1]) return Number(pa[1]) - Number(pb[1]);
          return Number(pa[2]) - Number(pb[2]);
        })
        .map((q) => {
          const vals = grouped.get(q) || [];
          const sVals = groupedSra.get(q) || [];
          const nVals = groupedNva.get(q) || [];
          return {
            quarter: q,
            count: vals.length,
            avg_close_days: vals.length ? Math.round((vals.reduce((a, b) => a + b, 0) / vals.length) * 10) / 10 : null,
            avg_sra_days: sVals.length ? Math.round((sVals.reduce((a, b) => a + b, 0) / sVals.length) * 10) / 10 : null,
            avg_nva_days: nVals.length ? Math.round((nVals.reduce((a, b) => a + b, 0) / nVals.length) * 10) / 10 : null,
          };
        });

      const historicalCompanies = new Set(historicalItems.map((r) => normalizeCompanyName(r.company)).filter(Boolean));
      const historicalSfIds = new Set(historicalItems.map((r) => String(r.sf_id || '').trim()).filter(Boolean));
      let coveredByHistorical = 0;
      let uncovered = 0;
      for (const m of missingRecords) {
        const company = String(m.company || '').trim();
        const sf = String(m.sf_id || '').trim();
        const covered = (sf && historicalSfIds.has(sf)) || historicalCompanies.has(normalizeCompanyName(company));
        if (covered) coveredByHistorical += 1;
        else uncovered += 1;
      }

      const liveQuality = {
        completed_task_total: completed.length,
        completed_task_with_valid_close: completedWithValidClose,
        completed_task_missing_close_dates: Math.max(completed.length - completedWithValidClose, 0),
        missing_sra_dates: missingSraDates,
        missing_nva_dates: missingNvaDates,
        missing_covered_by_historical: coveredByHistorical,
        missing_uncovered: uncovered,
      };
      const coveragePct = completed.length ? Math.round((100 * completedWithValidClose / completed.length) * 10) / 10 : null;

      return json(200, {
        summary: {
          total_tracked_closures: allRows.length,
          avg_close_days: avg,
          sra_avg_close_days: sraAvg,
          sra_count: sraRows.length,
          nva_avg_close_days: nvaAvg,
          nva_count: nvaRows.length,
          tracked_avg_days: trackedDays.length ? Math.round((trackedDays.reduce((a, b) => a + b, 0) / trackedDays.length) * 10) / 10 : null,
          tracked_count: trackedDays.length,
          untracked_outlier_count: Math.max(allDays.length - trackedDays.length, 0),
          tracked_max_days: TRACKED_MAX_CLOSE_DAYS,
          historical_count: historicalItems.length,
          live_count: liveDeduped.length,
          coverage_pct: coveragePct,
        },
        live_quality: liveQuality,
        quarters: quarterRows,
        rows: allRows.map((r) => ({
          company: r.company,
          track: r.track,
          quarter: r.quarter_label,
          days: r.close_days,
          source: r.source,
          kickoff_date: r.kickoff_date,
          final_date: r.final_date,
          sf_id: r.sf_id || '',
        })),
      });
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
