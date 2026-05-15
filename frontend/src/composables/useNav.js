import { reactive, watch } from 'vue';

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
});

watch(
  nav,
  (state) => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  },
  { deep: true },
);

export function setDetector(id) {
  nav.detectorId = id;
}
