// App-wide config populated at boot from GET /api/config. Reads from
// this module pick up the same object the bootstrap step wrote, so
// every fetch sees the right mode without prop-drilling.
const appConfig = { mode: 'local', loginUrl: null, llmEnabled: false };

// URL prefix to prepend to backend paths. Vite injects BASE_URL from
// `base` in vite.config.js — '/' for root mounts, '/<prefix>/' when
// built with VITE_BASE set. We strip the trailing slash so we can
// concatenate with paths that start with '/'.
const BASE = import.meta.env.BASE_URL.replace(/\/$/, '');

function withBase(path) {
  return /^https?:/.test(path) ? path : BASE + path;
}

export function setConfig(cfg) {
  appConfig.mode = cfg?.mode || 'local';
  appConfig.loginUrl = cfg?.login_url || null;
  appConfig.llmEnabled = !!cfg?.llm_enabled;
}

export function getConfig() {
  return { ...appConfig };
}

async function jsonFetch(url, options = {}) {
  const resp = await fetch(withBase(url), {
    credentials: 'same-origin', // session cookie rides along (Vite proxy is same-origin)
    ...options,
  });
  if (resp.status === 401 && appConfig.mode === 'hub') {
    window.location = appConfig.loginUrl || withBase('/hub/login');
    // Caller will see this throw, but the page is navigating away.
    throw new Error('redirecting to login');
  }
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({}));
    const err = new Error(body.detail || `HTTP ${resp.status}`);
    err.status = resp.status;
    throw err;
  }
  if (resp.status === 204) return null;
  return resp.json();
}

// Bare fetch, no 401-redirect — used at boot to learn the mode itself.
// If this fails, the SPA falls back to mode=local.
export async function fetchConfig() {
  try {
    const r = await fetch(withBase('/api/config'), { credentials: 'same-origin' });
    if (!r.ok) return { mode: 'local' };
    return await r.json();
  } catch {
    return { mode: 'local' };
  }
}

export async function logout() {
  await fetch(withBase('/hub/logout'), {
    method: 'POST',
    credentials: 'same-origin',
  });
}

function buildQuery(params) {
  const usp = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v == null || v === '') continue;
    if (Array.isArray(v)) {
      for (const item of v) usp.append(k, item);
    } else {
      usp.append(k, v);
    }
  }
  return usp.toString();
}

export const getMe = () => jsonFetch('/api/me');
export const getDetectors = () => jsonFetch('/api/detectors');
export const getDetectorCounts = () => jsonFetch('/api/detectors/counts');

export const getDatasets = (params) =>
  jsonFetch(`/api/datasets?${buildQuery(params)}`);

export const getDatasetsFacets = (params) =>
  jsonFetch(`/api/datasets/facets?${buildQuery(params)}`);

export const refreshDatasets = (detectorId) =>
  jsonFetch(`/api/datasets/refresh?${buildQuery({ detector: detectorId })}`, {
    method: 'POST',
  });

export const getFiles = (params) =>
  jsonFetch(`/api/files?${buildQuery(params)}`);

export const getFilesCount = (params) =>
  jsonFetch(`/api/files/count?${buildQuery(params)}`);

export const getFile = (did) => jsonFetch(`/api/file?${buildQuery({ did })}`);
export const getReplicas = (did) => jsonFetch(`/api/replicas?${buildQuery({ did })}`);
export const getDataset = (did) => jsonFetch(`/api/dataset?${buildQuery({ did })}`);
export const getRun = (run) => jsonFetch(`/api/run/${encodeURIComponent(run)}`);

export const getRunConditions = (detector, run) =>
  jsonFetch(
    `/api/runs/${encodeURIComponent(detector)}/${encodeURIComponent(run)}/conditions`,
  );

export const getRunsConditions = (detector, params) =>
  jsonFetch(
    `/api/runs/${encodeURIComponent(detector)}/conditions?${buildQuery(params)}`,
  );

export const getCondbColumns = (detector) =>
  jsonFetch(`/api/detectors/${encodeURIComponent(detector)}/condb-columns`);

export const runQuery = (mql, page, pageSize, savedQueryId = null) =>
  jsonFetch('/api/query/run', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      mql,
      page,
      page_size: pageSize,
      saved_query_id: savedQueryId,
    }),
  });

export const countQuery = (mql) =>
  jsonFetch('/api/query/count', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ mql }),
  });

export const validateQuery = (mql) =>
  jsonFetch('/api/query/validate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ mql }),
  });

export const queryFromEnglish = (english) =>
  jsonFetch('/api/query/from-english', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ english }),
  });

export const listSavedQueries = () => jsonFetch('/api/queries');

export const createSavedQuery = (name, mql) =>
  jsonFetch('/api/queries', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, mql }),
  });

export const updateSavedQuery = (id, patch) =>
  jsonFetch(`/api/queries/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(patch),
  });

export const deleteSavedQuery = (id) =>
  jsonFetch(`/api/queries/${id}`, { method: 'DELETE' });
