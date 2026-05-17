<script setup>
import { computed, onMounted, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { getDataset, getFiles, getFilesCount } from '../api.js';
import { loadDetectors, nav } from '../composables/useNav.js';
import { hasSelection } from '../composables/useRowNav.js';

const route = useRoute();
const router = useRouter();

const did = computed(() => route.params.did);
const namespace = computed(() => did.value?.split(':')[0] || '');
const name = computed(() => did.value?.split(':').slice(1).join(':') || '');

const detector = computed(() =>
  nav.detectors.find((d) => d.namespaces.includes(namespace.value)) || null,
);

const page = ref(1);
const pageSize = ref(100);
const PAGE_SIZES = [100, 200, 500];

const loading = ref(false);
const metadataLoading = ref(false);
const error = ref(null);
const data = ref(null);
const total = ref(null);            // null = unknown / loading / failed
const totalLoading = ref(false);

const dataset = ref(null);
const datasetLoading = ref(false);

let fetchToken = 0;                 // generation guard for in-flight fetches

const totalPages = computed(() => {
  if (total.value == null || !data.value) return null;
  return Math.max(1, Math.ceil(total.value / data.value.page_size));
});

const canNext = computed(() => {
  if (loading.value) return false;
  if (totalPages.value != null) return page.value < totalPages.value;
  return data.value?.has_more === true;
});

function fetchParams() {
  return { dataset: did.value };
}

async function fetchPage() {
  if (!did.value) return;
  const token = ++fetchToken;
  loading.value = true;
  error.value = null;

  // Fast pass: no metadata — table renders ASAP.
  try {
    const fast = await getFiles({
      ...fetchParams(),
      with_metadata: false,
      page: page.value,
      page_size: pageSize.value,
    });
    if (token !== fetchToken) return;
    data.value = fast;
  } catch (e) {
    if (token !== fetchToken) return;
    error.value = e.message;
    data.value = null;
    return;
  } finally {
    if (token === fetchToken) loading.value = false;
  }

  // Background pass: with metadata. Fills in Run/Events columns.
  metadataLoading.value = true;
  try {
    const full = await getFiles({
      ...fetchParams(),
      with_metadata: true,
      page: page.value,
      page_size: pageSize.value,
    });
    if (token !== fetchToken) return;
    data.value = full;
  } catch (_e) {
    // Background fetch failed; keep the fast result. Run/Events stay '—'.
  } finally {
    if (token === fetchToken) metadataLoading.value = false;
  }
}

async function fetchTotal() {
  if (!did.value) return;
  total.value = null;
  totalLoading.value = true;
  try {
    const r = await getFilesCount(fetchParams());
    total.value = r.total;
  } catch (_e) {
    total.value = null;
  } finally {
    totalLoading.value = false;
  }
}

function refreshAll() {
  fetchPage();
  fetchTotal();
}

watch(page, () => fetchPage());
watch(pageSize, () => {
  page.value = 1;
  refreshAll();
});

async function fetchDataset() {
  if (!did.value) return;
  datasetLoading.value = true;
  try {
    dataset.value = await getDataset(did.value);
  } catch (_e) {
    dataset.value = null;
  } finally {
    datasetLoading.value = false;
  }
}

watch(did, () => {
  fetchDataset();
  refreshAll();
});

onMounted(async () => {
  await loadDetectors();
  fetchDataset();
  refreshAll();
});

function openFile(fileDid) {
  if (hasSelection()) return;
  router.push({ name: 'file-detail', params: { did: fileDid } });
}

const copyState = ref('idle');  // 'idle' | 'copied' | 'failed'

// DIDs the user has explicitly ticked. Persisted in sessionStorage keyed
// by (dataset, page, pageSize) so the selection survives a remount when
// the user clicks a row by accident and hits the browser back button.
// Changing page or page size moves to a fresh key (those rows aren't on
// screen anymore), but returning to the same page restores the picks.
const selected = ref(new Set());
const selectionKey = computed(
  () => `files-selection:${did.value}:p${page.value}:s${pageSize.value}`,
);

watch(selectionKey, (key) => {
  try {
    const raw = sessionStorage.getItem(key);
    selected.value = raw ? new Set(JSON.parse(raw)) : new Set();
  } catch (_e) {
    selected.value = new Set();
  }
}, { immediate: true });

watch(selected, (s) => {
  try {
    if (s.size === 0) sessionStorage.removeItem(selectionKey.value);
    else sessionStorage.setItem(selectionKey.value, JSON.stringify([...s]));
  } catch (_e) { /* sessionStorage full or unavailable — fine, lose the persistence */ }
}, { deep: true });

const selectableDids = computed(
  () => (data.value?.rows || []).map((r) => `${r.namespace}:${r.name}`),
);
const allSelected = computed(
  () =>
    selectableDids.value.length > 0 &&
    selectableDids.value.every((d) => selected.value.has(d)),
);
const copyCount = computed(
  () => selected.value.size || selectableDids.value.length,
);

function toggleRow(did) {
  const next = new Set(selected.value);
  if (next.has(did)) next.delete(did);
  else next.add(did);
  selected.value = next;
}

function toggleAll() {
  if (allSelected.value) selected.value = new Set();
  else selected.value = new Set(selectableDids.value);
}

async function copyDids() {
  const dids = selected.value.size > 0
    ? [...selected.value]
    : selectableDids.value;
  if (!dids.length) return;
  try {
    await navigator.clipboard.writeText(dids.join('\n'));
    copyState.value = 'copied';
  } catch (_e) {
    copyState.value = 'failed';
  }
  setTimeout(() => { copyState.value = 'idle'; }, 1500);
}

function gotoDetector() {
  if (detector.value) {
    router.push({
      name: 'datasets-detector',
      params: { detectorId: detector.value.id },
    });
  }
}

function fmtNum(n) {
  if (n == null) return '—';
  return new Intl.NumberFormat().format(n);
}
function fmtBytes(n) {
  if (n == null) return '—';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0; let v = n;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i += 1; }
  return `${v.toFixed(v < 10 ? 2 : 1)} ${units[i]}`;
}
function fmtTimestamp(ts) {
  if (ts == null) return '—';
  const d = typeof ts === 'number' ? new Date(ts * 1000) : new Date(ts);
  return d.toISOString().slice(0, 16).replace('T', ' ');
}
function pickRun(row) {
  if (metadataLoading.value && !row.metadata) return '…';
  const runs = row.metadata?.['core.runs'];
  if (Array.isArray(runs) && runs.length) return runs[0];
  return '—';
}
function pickEvents(row) {
  if (metadataLoading.value && !row.metadata) return '…';
  const events = row.metadata?.['core.events'];
  if (Array.isArray(events)) return events.length;
  return '—';
}

const sortedDatasetMeta = computed(() => {
  if (!dataset.value?.metadata) return [];
  return Object.entries(dataset.value.metadata).sort(([a], [b]) => a.localeCompare(b));
});

function fmtTimestampLong(ts) {
  if (ts == null) return '—';
  const d = typeof ts === 'number' ? new Date(ts * 1000) : new Date(ts);
  return d.toISOString().slice(0, 16).replace('T', ' ') + ' UTC';
}
function fmtBool(b) {
  return b ? 'yes' : 'no';
}
function fmtMetaValue(v) {
  if (v == null) return '—';
  if (Array.isArray(v)) return v.join(', ');
  if (typeof v === 'object') return JSON.stringify(v);
  return String(v);
}
</script>

<template>
  <div class="page">
    <div class="header">
      <div class="eyebrow">
        <span
          v-if="detector"
          class="crumb crumb-link"
          @click="gotoDetector"
        >{{ detector.name }}</span>
        <span v-else class="crumb">{{ namespace }}</span>
        <span class="sep">›</span>
        <span class="crumb">{{ namespace }}:{{ name.slice(0, 40) }}{{ name.length > 40 ? '…' : '' }}</span>
      </div>
      <h1 class="title">{{ did }}</h1>
      <p class="subtitle" v-if="data">
        <strong v-if="total != null">{{ fmtNum(total) }}</strong>
        <strong v-else-if="totalLoading" class="loading-hint">counting…</strong>
        <strong v-else class="loading-hint">—</strong>
        files
      </p>
    </div>

    <div class="grid">
      <!-- Left: dataset metadata -->
      <aside class="rail">
        <div v-if="datasetLoading && !dataset" class="rail-empty">Loading dataset…</div>
        <template v-else-if="dataset">
          <div class="rail-section">
            <div class="rail-label">Dataset</div>
            <div class="rail-kv">
              <div class="rail-k">Creator</div>
              <div class="rail-v">{{ dataset.creator || '—' }}</div>
              <div class="rail-k">Created</div>
              <div class="rail-v">{{ fmtTimestampLong(dataset.created_timestamp) }}</div>
              <div class="rail-k">Frozen</div>
              <div class="rail-v">{{ fmtBool(dataset.frozen) }}</div>
            </div>
          </div>

          <div class="rail-section">
            <div class="rail-label">
              Metadata
              <span v-if="sortedDatasetMeta.length" class="rail-count">{{ sortedDatasetMeta.length }}</span>
            </div>
            <div v-if="sortedDatasetMeta.length" class="rail-kv">
              <template v-for="[k, v] in sortedDatasetMeta" :key="k">
                <div class="rail-k">{{ k }}</div>
                <div class="rail-v">{{ fmtMetaValue(v) }}</div>
              </template>
            </div>
            <div v-else class="rail-empty">No metadata fields set.</div>
          </div>
        </template>
        <div v-else class="rail-empty">Dataset info unavailable.</div>
      </aside>

      <!-- Main: file table -->
      <section class="main">
        <div v-if="error" class="error-card">
          <div class="error-title">Couldn't load files</div>
          <div class="error-detail">{{ error }}</div>
        </div>

        <div v-else-if="loading && !data" class="placeholder">
          <div class="placeholder-body">Loading…</div>
        </div>

        <div v-else-if="data && data.rows.length === 0" class="placeholder">
          <div class="placeholder-body">This dataset has no files.</div>
        </div>

        <template v-else-if="data">
          <div class="pager">
            <div class="pager-info">
              Showing <strong>{{ (page - 1) * pageSize + 1 }}</strong>–<strong>{{
                (page - 1) * pageSize + data.rows.length
              }}</strong><template v-if="total != null">
                of <strong>{{ fmtNum(total) }}</strong>
              </template>
            </div>
            <div class="pager-controls">
              <button
                class="btn"
                :disabled="!copyCount"
                :title="
                  selected.size > 0
                    ? `Copy the ${selected.size} selected file IDs (namespace:name, one per line). Paste into 'rucio list-file-replicas', 'metacat file show', etc.`
                    : `Copy all ${selectableDids.length} file IDs on this page (namespace:name, one per line). Paste into 'rucio list-file-replicas', 'metacat file show', etc.`
                "
                @click="copyDids"
              >
                <template v-if="copyState === 'copied'">Copied ✓</template>
                <template v-else-if="copyState === 'failed'">Copy failed</template>
                <template v-else>Copy {{ copyCount }} file IDs</template>
              </button>
              <select v-model="pageSize" class="page-size-select">
                <option v-for="s in PAGE_SIZES" :key="s" :value="s">
                  {{ s }} / page
                </option>
              </select>
              <button class="btn" :disabled="page <= 1 || loading" @click="page -= 1">
                ← Prev
              </button>
              <span class="pager-pos">
                Page {{ page }}<template v-if="totalPages != null"> / {{ totalPages }}</template>
              </span>
              <button
                class="btn"
                :disabled="!canNext"
                @click="page += 1"
              >
                Next →
              </button>
            </div>
          </div>

          <div class="table-card">
            <div class="table-head with-meta">
              <div class="th col-check">
                <input
                  type="checkbox"
                  :checked="allSelected"
                  :title="allSelected ? 'Deselect all on this page' : 'Select all on this page'"
                  @change="toggleAll"
                />
              </div>
              <div class="th col-name">File</div>
              <div class="th col-run">Run</div>
              <div class="th col-events">Events</div>
              <div class="th col-size">Size</div>
              <div class="th col-created">Created</div>
            </div>
            <div
              v-for="row in data.rows"
              :key="row.did"
              class="tr with-meta"
              @click="openFile(row.did)"
            >
              <div class="td col-check" @click.stop>
                <input
                  type="checkbox"
                  :checked="selected.has(`${row.namespace}:${row.name}`)"
                  @change="toggleRow(`${row.namespace}:${row.name}`)"
                />
              </div>
              <div class="td col-name" :title="`${row.namespace}:${row.name}`">
                <span class="ns">{{ row.namespace }}:</span><span class="nm">{{ row.name }}</span>
              </div>
              <div class="td col-run">{{ pickRun(row) }}</div>
              <div class="td col-events">{{ fmtNum(pickEvents(row)) }}</div>
              <div class="td col-size">{{ fmtBytes(row.size) }}</div>
              <div class="td col-created">{{ fmtTimestamp(row.created_timestamp) }}</div>
            </div>
          </div>
        </template>
      </section>
    </div>
  </div>
</template>

<style scoped>
.page { padding: 22px 36px 28px; }

.header { padding-bottom: 16px; border-bottom: 1px solid var(--rule); margin-bottom: 20px; }
.eyebrow {
  font-family: var(--font-mono);
  font-size: 11.5px;
  color: var(--faint);
  margin-bottom: 6px;
  display: flex;
  align-items: center;
  gap: 6px;
}
.crumb { color: var(--faint); }
.crumb-link { cursor: pointer; }
.crumb-link:hover { color: var(--ink); }
.sep { color: var(--faint); }

.title {
  font-family: var(--font-mono);
  font-size: 16px;
  font-weight: 500;
  color: var(--ink);
  margin: 0 0 4px;
  word-break: break-all;
  line-height: 1.4;
}
.subtitle {
  font-size: 13px;
  color: var(--dim);
  margin: 0;
}

.grid {
  display: grid;
  grid-template-columns: 280px 1fr;
  gap: 24px;
}

/* Left rail: dataset metadata */
.rail {
  display: flex;
  flex-direction: column;
  gap: 18px;
  font-size: 12px;
}
.rail-section {
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.rail-label {
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.6px;
  text-transform: uppercase;
  color: var(--faint);
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.rail-count {
  font-family: var(--font-mono);
  font-size: 11px;
  color: var(--faint);
  font-weight: 400;
  letter-spacing: 0;
  text-transform: none;
}
.rail-kv {
  display: grid;
  grid-template-columns: minmax(0, max-content) minmax(0, 1fr);
  gap: 3px 10px;
}
.rail-k {
  font-size: 11px;
  color: var(--dim);
  white-space: nowrap;
}
.rail-v {
  font-family: var(--font-mono);
  font-size: 11.5px;
  color: var(--ink);
  word-break: break-word;
}
.rail-desc {
  display: block;
  background: var(--surface);
  border-radius: 6px;
  padding: 8px 10px;
  font-family: var(--font-mono);
  font-size: 11.5px;
  color: var(--ink);
  word-break: break-word;
  line-height: 1.5;
}
.rail-empty {
  font-size: 11.5px;
  color: var(--faint);
  font-style: italic;
}

/* Buttons */
.btn {
  display: inline-flex;
  align-items: center;
  height: 28px;
  padding: 0 12px;
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
.btn-primary { background: var(--ink); border-color: var(--ink); color: white; }

/* Table */
.main { display: flex; flex-direction: column; gap: 14px; min-width: 0; }
.table-card {
  background: var(--page);
  border: 1px solid var(--rule);
  border-radius: 10px;
  overflow-x: auto;
}
.table-head, .tr {
  display: grid;
  grid-template-columns: 28px 1fr 90px 110px;
  gap: 12px;
  padding: 8px 16px;
  align-items: center;
  min-width: 748px;
}
.table-head.with-meta, .tr.with-meta {
  grid-template-columns: 28px 1fr 70px 80px 90px 110px;
}
.col-check {
  display: flex;
  align-items: center;
  justify-content: center;
}
.col-check input[type="checkbox"] {
  margin: 0;
  cursor: pointer;
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

.td { font-size: 12px; color: var(--ink); }
.col-name {
  font-family: var(--font-mono);
  word-break: break-all;
  min-width: 0;
  line-height: 1.4;
}
.col-name .ns { color: var(--faint); font-size: 10.5px; }
.col-name .nm { color: var(--ink); }
.col-run, .col-events, .col-size, .col-created {
  font-family: var(--font-mono);
  text-align: right;
  color: var(--dim);
}

/* Pager */
.pager {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0 4px;
  flex-wrap: wrap;
  gap: 10px;
}
.pager-info { font-size: 12px; color: var(--dim); }
.pager-controls { display: flex; align-items: center; gap: 10px; }
.pager-pos {
  font-family: var(--font-mono);
  font-size: 12px;
  color: var(--dim);
  min-width: 90px;
  text-align: center;
}
.page-size-select {
  height: 28px;
  padding: 0 8px;
  border: 1px solid var(--rule);
  background: var(--page);
  border-radius: 7px;
  font-family: var(--font-sans);
  font-size: 12.5px;
  color: var(--body);
}

/* Placeholders */
.placeholder {
  padding: 28px;
  background: var(--page);
  border: 1px solid var(--rule);
  border-radius: 10px;
  text-align: center;
}
.placeholder-body { font-size: 13.5px; color: var(--dim); }

.error-card {
  padding: 14px 16px;
  background: var(--bad-bg);
  border: 1px solid var(--bad);
  border-radius: 10px;
}
.error-title { font-weight: 600; color: var(--bad); margin-bottom: 4px; }
.error-detail { font-family: var(--font-mono); font-size: 12px; color: var(--ink); }

.loading-hint { color: var(--faint); font-weight: 400; font-style: italic; }
</style>
