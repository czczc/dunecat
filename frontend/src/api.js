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
