const crypto = require('crypto');

function required(name) {
  const value = (process.env[name] || '').trim();
  if (!value) throw new Error(`Missing env var: ${name}`);
  return value;
}

function parseFieldMap() {
  try {
    const raw = process.env.CLICKUP_FIELD_MAP_JSON || '{}';
    const parsed = JSON.parse(raw);
    return typeof parsed === 'object' && parsed ? parsed : {};
  } catch {
    return {};
  }
}

function toDateUS(ms) {
  if (!ms) return '';
  const n = Number(ms);
  if (!Number.isFinite(n)) return '';
  const d = new Date(n);
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const yyyy = d.getFullYear();
  return `${mm}/${dd}/${yyyy}`;
}

function parseUSDate(text) {
  const value = String(text || '').trim();
  const m = value.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (!m) return null;
  const dt = new Date(Number(m[3]), Number(m[1]) - 1, Number(m[2]));
  return Number.isNaN(dt.getTime()) ? null : dt;
}

function dateDiffBusinessDays(start, end) {
  if (!start || !end) return null;
  const a = new Date(start.getFullYear(), start.getMonth(), start.getDate());
  const b = new Date(end.getFullYear(), end.getMonth(), end.getDate());
  if (b < a) return null;
  let count = 0;
  const cur = new Date(a);
  while (cur <= b) {
    const day = cur.getDay();
    if (day !== 0 && day !== 6) count += 1;
    cur.setDate(cur.getDate() + 1);
  }
  return count;
}

function quarterLabel(date) {
  const q = Math.floor(date.getMonth() / 3) + 1;
  return `${date.getFullYear()} Q${q}`;
}

function normalizeText(value) {
  return String(value || '').trim().toLowerCase();
}

function sign(sfId) {
  const secret = required('CLIENT_LINK_SECRET');
  return crypto.createHmac('sha256', secret).update(String(sfId)).digest('hex');
}

function readHeader(headers, key) {
  if (!headers) return '';
  if (typeof headers.get === 'function') return String(headers.get(key) || '').trim();
  return String(headers[key] || headers[String(key).toLowerCase()] || '').trim();
}

function requestHost(req) {
  const host = readHeader(req.headers, 'x-forwarded-host') || readHeader(req.headers, 'host');
  return String(host || '').trim().toLowerCase().split(',')[0].trim();
}

function hostAllowedForAdmin(req) {
  const allowRaw = String(process.env.ADMIN_ALLOWED_HOSTS || '').trim();
  if (!allowRaw) return true;
  const host = requestHost(req);
  if (!host) return false;
  const allowed = allowRaw
    .split(',')
    .map((h) => h.trim().toLowerCase())
    .filter(Boolean);
  return allowed.some((h) => host === h || host.startsWith(`${h}:`));
}

function isAdmin(req) {
  if (!hostAllowedForAdmin(req)) return false;
  const configured = (process.env.ADMIN_API_KEY || '').trim();
  if (!configured) return true;
  const provided = String(req.query.key || readHeader(req.headers, 'x-api-key') || '').trim();
  return provided === configured;
}

module.exports = {
  required,
  parseFieldMap,
  toDateUS,
  parseUSDate,
  dateDiffBusinessDays,
  quarterLabel,
  normalizeText,
  sign,
  isAdmin,
};
