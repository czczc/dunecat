<script setup>
import { computed, onMounted, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { getDatasets, refreshDatasets } from '../api.js';
import {
  nav,
  loadDetectors,
  loadCounts,
  setDetector,
} from '../composables/useNav.js';

const route = useRoute();
const router = useRouter();

const detectorId = computed(() => route.params.detectorId);
const activeDetector = computed(
  () => nav.detectors.find((d) => d.id === detectorId.value) || null,
);
const activeCounts = computed(() => nav.counts[detectorId.value] || null);

const pattern = ref('');
const tier = ref('all');
const officialOnly = ref(true);
const withMetadataOnly = ref(true);
const page = ref(1);
const PAGE_SIZE = 100;

const loading = ref(false);
const error = ref(null);
const data = ref(null);

const TIERS = [
  { id: 'all', label: 'All tiers', value: null },
  { id: 'raw', label: 'raw', value: 'raw' },
  { id: 'reco', label: 'reco', value: 'full-reconstructed' },
  { id: 'mc', label: 'mc', value: 'mc' },
  { id: 'cal', label: 'cal', value: 'cal' },
];

let debounceTimer = null;

async function fetchPage() {
  if (!detectorId.value) {
    data.value = null;
    return;
  }
  loading.value = true;
  error.value = null;
  try {
    const tierValue = TIERS.find((t) => t.id === tier.value)?.value;
    data.value = await getDatasets({
      detector: detectorId.value,
      pattern: pattern.value || null,
      tier: tierValue,
      official_only: officialOnly.value,
      with_metadata_only: withMetadataOnly.value,
      page: page.value,
      page_size: PAGE_SIZE,
    });
  } catch (e) {
    error.value = e.message;
    data.value = null;
  } finally {
    loading.value = false;
  }
}

watch([detectorId, tier, officialOnly, withMetadataOnly], () => {
  page.value = 1;
  fetchPage();
});

watch(pattern, () => {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(() => {
    page.value = 1;
    fetchPage();
  }, 300);
});

watch(page, fetchPage);

async function onRefresh() {
  if (!detectorId.value) return;
  loading.value = true;
  try {
    await refreshDatasets(detectorId.value);
    await fetchPage();
    await loadCounts({ force: true });
  } finally {
    loading.value = false;
  }
}

function openDataset(did) {
  router.push({ name: 'dataset-files', params: { did } });
}

function selectDetector(id) {
  setDetector(id);
  router.push({ name: 'datasets-detector', params: { detectorId: id } });
}

onMounted(async () => {
  await loadDetectors();
  loadCounts();
  fetchPage();
});

function fmtNum(n) {
  if (n == null) return '—';
  return new Intl.NumberFormat().format(n);
}

function fmtBytes(n) {
  if (n == null) return '—';
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  let i = 0;
  let v = n;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${v.toFixed(v < 10 ? 1 : 0)} ${units[i]}`;
}

function fmtTimestamp(ts) {
  if (ts == null) return '—';
  const d = typeof ts === 'number' ? new Date(ts * 1000) : new Date(ts);
  return d.toISOString().slice(0, 10);
}

function fmtRuns(runs) {
  let arr = null;
  if (Array.isArray(runs)) {
    arr = runs.filter((r) => Number.isFinite(Number(r))).map(Number);
  } else if (typeof runs === 'string') {
    const nums = runs.match(/\d+/g);
    if (nums) arr = nums.map(Number);
  }
  if (!arr || arr.length === 0) return '—';
  let min = arr[0];
  let max = arr[0];
  for (const r of arr) {
    if (r < min) min = r;
    if (r > max) max = r;
  }
  return min === max ? `${min}` : `${min}–${max}`;
}

function fmtRelative(iso) {
  if (!iso) return '';
  const then = new Date(iso).getTime();
  const now = Date.now();
  const sec = Math.max(0, Math.floor((now - then) / 1000));
  if (sec < 60) return `${sec}s ago`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m ago`;
  if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`;
  return `${Math.floor(sec / 86400)}d ago`;
}

const totalPages = computed(() =>
  data.value ? Math.max(1, Math.ceil(data.value.total / data.value.page_size)) : 1,
);
</script>

<template>
  <div class="page">
    <div class="header">
      <div class="eyebrow">Datasets</div>
      <h1 class="title">Browse datasets</h1>
      <p class="subtitle">
        Pick a sub-detector, then open a dataset to see its files.
      </p>
    </div>

    <!-- Detector picker -->
    <div class="detector-bar">
      <div v-if="nav.detectors.length === 0" class="detector-loading">Loading detectors…</div>
      <button
        v-for="d in nav.detectors"
        :key="d.id"
        class="detector-chip"
        :class="{ active: d.id === detectorId }"
        @click="selectDetector(d.id)"
      >
        <span class="detector-name">{{ d.name }}</span>
        <span class="detector-count" :class="{ pending: nav.countsLoading && !nav.counts[d.id] }">
          {{ fmtNum(nav.counts[d.id]?.datasets_count) }}
        </span>
      </button>
    </div>

    <template v-if="!detectorId">
      <div class="placeholder">
        <div class="placeholder-title">Pick a detector to start</div>
        <div class="placeholder-body">
          Choose a detector above to browse its datasets.
        </div>
      </div>
    </template>

    <template v-else>
      <!-- Detector hero card -->
      <div class="hero" v-if="activeDetector">
        <div class="hero-main">
          <div class="hero-name">{{ activeDetector.name }}</div>
          <div class="hero-namespaces">
            <span class="ns-label">namespaces</span>
            <code
              v-for="ns in activeDetector.namespaces"
              :key="ns"
              class="ns-tag"
            >{{ ns }}</code>
          </div>
        </div>
        <div class="hero-stats">
          <div class="stat">
            <div class="stat-label">Datasets</div>
            <div class="stat-value">
              {{ activeCounts ? fmtNum(activeCounts.datasets_count) : '…' }}
            </div>
          </div>
          <div class="stat">
            <div class="stat-label">Files</div>
            <div class="stat-value">
              {{ activeCounts ? fmtNum(activeCounts.files_count) : '…' }}
            </div>
          </div>
        </div>
      </div>

      <!-- Filter row -->
      <div class="filters">
        <div class="chips">
          <button
            v-for="t in TIERS"
            :key="t.id"
            class="chip"
            :class="{ active: tier === t.id }"
            @click="tier = t.id"
          >
            {{ t.label }}
          </button>
          <span class="chip-divider" />
          <button
            class="chip"
            :class="{ active: officialOnly }"
            @click="officialOnly = !officialOnly"
            :title="officialOnly ? 'Showing only datasets created by dunepro' : 'Showing datasets from all creators'"
          >
            Official only
          </button>
          <button
            class="chip"
            :class="{ active: withMetadataOnly }"
            @click="withMetadataOnly = !withMetadataOnly"
            :title="withMetadataOnly ? 'Hiding datasets that report no metadata (often test datasets)' : 'Showing all datasets, including those with empty metadata'"
          >
            With metadata
          </button>
        </div>
        <input
          v-model="pattern"
          type="text"
          class="search"
          placeholder="fnmatch pattern — e.g. *cosmic* or *beam_2024*"
        />
      </div>

      <!-- Toolbar: refresh on the left, pager on the right -->
      <div class="toolbar" v-if="data && data.total > 0">
        <div class="toolbar-left">
          <button class="btn" :disabled="loading" @click="onRefresh">
            {{ loading ? 'Refreshing…' : 'Refresh' }}
          </button>
          <span class="fetched">Fetched {{ fmtRelative(data.fetched_at) }}</span>
        </div>
        <div class="pager-controls">
          <span class="pager-info">
            Showing
            <strong>{{ (page - 1) * PAGE_SIZE + 1 }}</strong>
            –<strong>{{ Math.min(page * PAGE_SIZE, data.total) }}</strong>
            of <strong>{{ fmtNum(data.total) }}</strong>
          </span>
          <button class="btn" :disabled="page <= 1 || loading" @click="page -= 1">
            ← Prev
          </button>
          <span class="pager-pos">Page {{ page }} / {{ totalPages }}</span>
          <button
            class="btn"
            :disabled="page >= totalPages || loading"
            @click="page += 1"
          >
            Next →
          </button>
        </div>
      </div>

      <!-- Error -->
      <div v-if="error" class="error-card">
        <div class="error-title">Couldn't load datasets</div>
        <div class="error-detail">{{ error }}</div>
      </div>

      <!-- Loading -->
      <div v-else-if="loading && !data" class="placeholder">
        <div class="placeholder-body">Loading…</div>
      </div>

      <!-- Empty -->
      <div v-else-if="data && data.total === 0" class="placeholder">
        <div class="placeholder-body">No datasets match these filters.</div>
      </div>

      <!-- Table + pagination -->
      <template v-else-if="data">
        <div class="table-card">
          <div class="table-head">
            <div class="th col-name">Dataset</div>
            <div class="th col-runs">Runs</div>
            <div class="th col-files">Files</div>
            <div class="th col-tier">Tier</div>
            <div class="th col-updated">Updated</div>
          </div>
          <div
            v-for="row in data.rows"
            :key="row.did"
            class="tr"
            @click="openDataset(row.did)"
          >
            <div class="td col-name">
              <span class="ns">{{ row.namespace }}</span><span class="sep">:</span><span class="nm">{{ row.name }}</span>
            </div>
            <div class="td col-runs">{{ fmtRuns(row.metadata['core.runs']) }}</div>
            <div class="td col-files">{{ fmtNum(row.file_count) }}</div>
            <div class="td col-tier">{{ row.metadata['core.data_tier'] || '—' }}</div>
            <div class="td col-updated">{{ fmtTimestamp(row.updated_timestamp || row.created_timestamp) }}</div>
          </div>
        </div>
      </template>
    </template>
  </div>
</template>

<style scoped>
.page { padding: 22px 36px 28px; }

/* Page header */
.header { padding-bottom: 16px; border-bottom: 1px solid var(--rule); margin-bottom: 20px; }
.eyebrow {
  font-family: var(--font-mono);
  font-size: 11.5px;
  color: var(--faint);
  margin-bottom: 4px;
}
.title {
  font-size: 22px;
  font-weight: 600;
  letter-spacing: -0.4px;
  margin: 0 0 4px;
  color: var(--ink);
}
.subtitle { font-size: 13.5px; color: var(--dim); margin: 0; }

/* Detector chip bar */
.detector-bar {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 14px;
}
.detector-loading {
  font-size: 12.5px;
  color: var(--faint);
  font-style: italic;
}
.detector-chip {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  height: 30px;
  padding: 0 12px;
  border: 1px solid var(--rule);
  background: var(--page);
  border-radius: 999px;
  cursor: pointer;
  font: inherit;
  transition: background 0.12s, border-color 0.12s;
}
.detector-chip:hover { background: var(--surface); }
.detector-chip.active {
  border-color: var(--accent);
  background: var(--accent-soft);
}
.detector-chip.active .detector-name { color: var(--accent-ink); font-weight: 600; }
.detector-name {
  font-size: 13px;
  font-weight: 500;
  color: var(--ink);
}
.detector-count {
  font-family: var(--font-mono);
  font-size: 11px;
  color: var(--faint);
}
.detector-count.pending { opacity: 0.4; }

/* Detector hero */
.hero {
  display: flex;
  align-items: center;
  gap: 28px;
  padding: 14px 18px;
  background: var(--page);
  border: 1px solid var(--rule);
  border-radius: 10px;
  margin-bottom: 16px;
}
.hero-main { flex: 1; min-width: 0; }
.hero-name { font-size: 17px; font-weight: 600; color: var(--ink); }
.hero-namespaces {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 6px;
  margin-top: 6px;
}
.ns-label {
  font-size: 10.5px;
  font-weight: 600;
  letter-spacing: 0.6px;
  text-transform: uppercase;
  color: var(--faint);
  margin-right: 2px;
}
.ns-tag {
  font-family: var(--font-mono);
  font-size: 11px;
  color: var(--dim);
  background: var(--surface);
  border: 1px solid var(--rule);
  padding: 1px 6px;
  border-radius: 4px;
}
.hero-stats { display: flex; gap: 28px; }
.stat-label {
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.6px;
  text-transform: uppercase;
  color: var(--faint);
}
.stat-value { font-size: 18px; font-weight: 600; color: var(--ink); }

/* Filter chips */
.filters {
  display: flex;
  align-items: center;
  gap: 16px;
  margin-bottom: 12px;
  flex-wrap: wrap;
}
.chips { display: flex; align-items: center; gap: 6px; }
.chip-divider {
  width: 1px;
  height: 18px;
  background: var(--rule);
  margin: 0 4px;
}
.chip {
  padding: 5px 10px;
  border-radius: 999px;
  border: 1px solid var(--rule);
  background: var(--page);
  color: var(--body);
  font-family: var(--font-sans);
  font-size: 12.5px;
  font-weight: 500;
  cursor: pointer;
  transition: background 0.12s;
}
.chip:hover { background: var(--surface); }
.chip.active {
  border-color: var(--accent);
  background: var(--accent-soft);
  color: var(--accent-ink);
  font-weight: 600;
}
.search {
  flex: 1;
  min-width: 280px;
  height: 32px;
  padding: 0 12px;
  border: 1px solid var(--rule);
  background: var(--page);
  border-radius: 8px;
  font-family: var(--font-mono);
  font-size: 12.5px;
  color: var(--body);
  outline: none;
}
.search:focus { border-color: var(--accent); }

/* Toolbar above table: refresh on left, pager on right */
.toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 8px;
  font-size: 12px;
  flex-wrap: wrap;
  gap: 10px;
}
.toolbar-left { display: flex; align-items: center; gap: 10px; }
.fetched { color: var(--faint); font-family: var(--font-mono); font-size: 12px; }

/* Generic btn */
.btn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  height: 28px;
  padding: 0 10px;
  border: 1px solid var(--rule);
  background: var(--page);
  color: var(--body);
  font-family: var(--font-sans);
  font-size: 12.5px;
  font-weight: 500;
  border-radius: 7px;
  cursor: pointer;
  transition: background 0.12s;
}
.btn:hover:not(:disabled) { background: var(--surface); }
.btn:disabled { opacity: 0.5; cursor: not-allowed; }

/* Table */
.table-card {
  background: var(--page);
  border: 1px solid var(--rule);
  border-radius: 10px;
  overflow: hidden;
}
.table-head, .tr {
  display: grid;
  grid-template-columns: 2.4fr 130px 100px 140px 110px;
  gap: 12px;
  padding: 8px 16px;
  align-items: center;
}
.table-head {
  background: var(--page);
  border-bottom: 1px solid var(--rule);
}
.th {
  font-family: var(--font-sans);
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.6px;
  text-transform: uppercase;
  color: var(--faint);
}
.tr {
  border-top: 1px solid var(--rule-soft);
  cursor: pointer;
  transition: background 0.12s;
}
.tr:hover { background: var(--surface); }
.td, .th { min-width: 0; }
.td { font-size: 12.5px; color: var(--ink); }
.col-name { overflow-wrap: anywhere; }
.col-name .ns {
  font-family: var(--font-mono);
  color: var(--faint);
  font-size: 10.5px;
}
.col-name .sep { color: var(--faint); margin: 0 2px; }
.col-name .nm { font-family: var(--font-sans); font-weight: 500; }
.col-runs, .col-files, .col-tier, .col-updated {
  font-family: var(--font-mono);
  text-align: right;
  color: var(--dim);
}

/* Pager (inside toolbar) */
.pager-info { font-size: 12px; color: var(--dim); }
.pager-controls { display: flex; align-items: center; gap: 10px; }
.pager-pos {
  font-family: var(--font-mono);
  font-size: 12px;
  color: var(--dim);
  min-width: 90px;
  text-align: center;
}

/* Placeholders */
.placeholder {
  padding: 28px;
  background: var(--page);
  border: 1px solid var(--rule);
  border-radius: 10px;
  text-align: center;
}
.placeholder-title {
  font-size: 14px;
  font-weight: 600;
  color: var(--ink);
  margin-bottom: 6px;
}
.placeholder-body {
  font-size: 13.5px;
  color: var(--dim);
}

.error-card {
  padding: 14px 16px;
  background: var(--bad-bg);
  border: 1px solid var(--bad);
  border-radius: 10px;
}
.error-title { font-weight: 600; color: var(--bad); margin-bottom: 4px; }
.error-detail { font-family: var(--font-mono); font-size: 12px; color: var(--ink); }
</style>
