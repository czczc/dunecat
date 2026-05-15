async function jsonFetch(url, options = {}) {
  const resp = await fetch(url, options);
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({}));
    const err = new Error(body.detail || `HTTP ${resp.status}`);
    err.status = resp.status;
    throw err;
  }
  if (resp.status === 204) return null;
  return resp.json();
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

export const getDetectors = () => jsonFetch('/api/detectors');
export const getDetectorCounts = () => jsonFetch('/api/detectors/counts');

export const getDatasets = (params) =>
  jsonFetch(`/api/datasets?${buildQuery(params)}`);

export const refreshDatasets = (detectorId) =>
  jsonFetch(`/api/datasets/refresh?${buildQuery({ detector: detectorId })}`, {
    method: 'POST',
  });

export const getFiles = (params) =>
  jsonFetch(`/api/files?${buildQuery(params)}`);

export const getFilesCount = (params) =>
  jsonFetch(`/api/files/count?${buildQuery(params)}`);

export const getFile = (did) => jsonFetch(`/api/file?${buildQuery({ did })}`);
export const getDataset = (did) => jsonFetch(`/api/dataset?${buildQuery({ did })}`);

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
