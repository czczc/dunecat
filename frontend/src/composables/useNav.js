import { reactive, watch } from 'vue';
import { getDetectorCounts, getDetectors } from '../api.js';

const STORAGE_KEY = 'dunecat.nav.v1';

function loadInitial() {
  try {
    return JSON.parse(localStorage.getItem(STORAGE_KEY) || '{}');
  } catch {
    return {};
  }
}

const stored = loadInitial();

export const nav = reactive({
  detectorId: stored.detectorId || null,
  detectors: [],          // [{id, name, namespaces}]
  counts: {},             // {detectorId: {datasets_count, files_count}}
  countsLoading: false,
  countsError: null,
});

watch(
  () => ({ detectorId: nav.detectorId }),
  (state) => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  },
  { deep: true },
);

export function setDetector(id) {
  nav.detectorId = id;
}

export async function loadDetectors() {
  if (nav.detectors.length > 0) return nav.detectors;
  nav.detectors = await getDetectors();
  return nav.detectors;
}

export async function loadCounts({ force = false } = {}) {
  if (!force && Object.keys(nav.counts).length > 0) return nav.counts;
  nav.countsLoading = true;
  nav.countsError = null;
  try {
    const data = await getDetectorCounts();
    nav.counts = Object.fromEntries(data.map((c) => [c.id, c]));
  } catch (e) {
    nav.countsError = e.message;
  } finally {
    nav.countsLoading = false;
  }
  return nav.counts;
}

export function detectorById(id) {
  return nav.detectors.find((d) => d.id === id) || null;
}
