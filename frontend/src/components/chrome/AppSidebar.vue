<script setup>
import { onMounted, ref, computed } from 'vue';
import { useRouter, useRoute } from 'vue-router';
import { nav, setDetector } from '../../composables/useNav.js';

const router = useRouter();
const route = useRoute();
const detectors = ref([]);
const loadError = ref(null);
const loading = ref(true);

const activeDetectorId = computed(
  () => route.params.detectorId || nav.detectorId,
);

async function fetchDetectors() {
  loading.value = true;
  loadError.value = null;
  try {
    const resp = await fetch('/api/detectors');
    if (!resp.ok) {
      const body = await resp.json().catch(() => ({}));
      throw new Error(body.detail || `HTTP ${resp.status}`);
    }
    detectors.value = await resp.json();
  } catch (e) {
    loadError.value = e.message;
  } finally {
    loading.value = false;
  }
}

function selectDetector(id) {
  setDetector(id);
  router.push({ name: 'datasets-detector', params: { detectorId: id } });
}

function fmt(n) {
  if (n == null) return '—';
  return new Intl.NumberFormat().format(n);
}

onMounted(fetchDetectors);
</script>

<template>
  <aside class="sidebar">
    <div class="section-label">
      Detectors
      <span class="badge">{{ detectors.length || '' }}</span>
    </div>

    <div v-if="loading" class="status">Loading…</div>
    <div v-else-if="loadError" class="error">
      <div class="error-title">Couldn't load detectors</div>
      <div class="error-detail">{{ loadError }}</div>
    </div>

    <ul v-else class="list">
      <li
        v-for="d in detectors"
        :key="d.id"
        class="row"
        :class="{ active: d.id === activeDetectorId }"
        @click="selectDetector(d.id)"
      >
        <span class="dot" />
        <span class="name">{{ d.name }}</span>
        <span class="count">{{ fmt(d.datasets_count) }}</span>
      </li>
    </ul>
  </aside>
</template>

<style scoped>
.sidebar {
  width: 252px;
  flex-shrink: 0;
  padding: 20px 14px;
  background: var(--page);
  border-right: 1px solid var(--rule);
  overflow-y: auto;
}

.section-label {
  display: flex;
  align-items: center;
  justify-content: space-between;
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.6px;
  text-transform: uppercase;
  color: var(--faint);
  margin-bottom: 10px;
  padding: 0 4px;
}
.badge {
  font-family: var(--font-mono);
  font-size: 10.5px;
  font-weight: 400;
  color: var(--faint);
  letter-spacing: 0;
  text-transform: none;
}

.list { list-style: none; padding: 0; margin: 0; }
.row {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 7px 10px;
  border-radius: 6px;
  cursor: pointer;
  transition: background 0.12s;
}
.row:hover { background: var(--surface); }
.row.active {
  background: var(--accent-soft);
  color: var(--accent-ink);
}
.row.active .name { font-weight: 600; }

.dot {
  width: 6px; height: 6px;
  border-radius: 50%;
  background: var(--faint);
  flex-shrink: 0;
}
.row.active .dot { background: var(--accent); }

.name {
  flex: 1;
  font-size: 13px;
  font-weight: 500;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.count {
  font-family: var(--font-mono);
  font-size: 11px;
  color: var(--faint);
}

.status, .error {
  padding: 8px 10px;
  font-size: 12.5px;
  color: var(--dim);
}
.error-title {
  font-weight: 600;
  color: var(--bad);
  margin-bottom: 4px;
}
.error-detail {
  font-family: var(--font-mono);
  font-size: 11px;
  color: var(--dim);
  white-space: pre-wrap;
  word-break: break-word;
}
</style>
