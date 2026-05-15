<script setup>
import { computed, onMounted, reactive, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { getDataset, getFiles, getFilesCount } from '../api.js';
import { loadDetectors, nav } from '../composables/useNav.js';

const route = useRoute();
const router = useRouter();

const did = computed(() => route.params.did);
const namespace = computed(() => did.value?.split(':')[0] || '');
const name = computed(() => did.value?.split(':').slice(1).join(':') || '');

const detector = computed(() =>
  nav.detectors.find((d) => d.namespaces.includes(namespace.value)) || null,
);

// Filter form state — only applied on "Apply"
const draft = reactive({
  runs: '',
  runRange: '',
  namespaceOverride: '',
  withMetadata: true,
  metaRows: [],
});
const applied = reactive({
  runs: '',
  runRange: '',
  namespaceOverride: '',
  withMetadata: true,
  metaRows: [],
});

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
  // total unknown: rely on has_more from the page response
  return data.value?.has_more === true;
});

function currentFilterParams() {
  const meta = applied.metaRows
    .filter((r) => r.key && r.value)
    .map((r) => `${r.key}=${r.value}`);
  return {
    dataset: did.value,
    runs: applied.runs || null,
    run_range: applied.runRange || null,
    namespace: applied.namespaceOverride || null,
    meta: meta.length ? meta : null,
  };
}

async function fetchPage() {
  if (!did.value) return;
  const token = ++fetchToken;
  loading.value = true;
  error.value = null;

  // Fast pass: no metadata — table renders ASAP.
  try {
    const fast = await getFiles({
      ...currentFilterParams(),
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
  if (!applied.withMetadata) return;
  metadataLoading.value = true;
  try {
    const full = await getFiles({
      ...currentFilterParams(),
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
    const r = await getFilesCount(currentFilterParams());
    total.value = r.total;
  } catch (_e) {
    total.value = null;  // surfaces as "—" in the UI; not a hard error
  } finally {
    totalLoading.value = false;
  }
}

function refreshAll() {
  fetchPage();
  fetchTotal();
}

function applyFilters() {
  Object.assign(applied, {
    runs: draft.runs,
    runRange: draft.runRange,
    namespaceOverride: draft.namespaceOverride,
    withMetadata: draft.withMetadata,
    metaRows: draft.metaRows.map((r) => ({ ...r })),
  });
  page.value = 1;
  refreshAll();
}

function clearFilters() {
  draft.runs = '';
  draft.runRange = '';
  draft.namespaceOverride = '';
  draft.withMetadata = false;
  draft.metaRows = [];
  applyFilters();
}

function addMetaRow() {
  draft.metaRows.push({ key: '', value: '' });
}
function removeMetaRow(i) {
  draft.metaRows.splice(i, 1);
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
  router.push({ name: 'file-detail', params: { did: fileDid } });
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
  if (applied.withMetadata && metadataLoading.value && !row.metadata) return '…';
  const runs = row.metadata?.['core.runs'];
  if (Array.isArray(runs) && runs.length) return runs[0];
  return '—';
}
function pickEvents(row) {
  if (applied.withMetadata && metadataLoading.value && !row.metadata) return '…';
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

    <details v-if="dataset" class="ds-info">
      <summary class="ds-info-summary">
        <span class="ds-info-label">Dataset metadata</span>
        <span class="ds-info-stats">
          {{ dataset.creator || '—' }} · created {{ fmtTimestampLong(dataset.created_timestamp) }}
          · {{ fmtBool(dataset.frozen) }}-frozen
          <template v-if="sortedDatasetMeta.length">· {{ sortedDatasetMeta.length }} fields</template>
        </span>
      </summary>
      <div class="ds-info-body">
        <div v-if="dataset.description" class="ds-description">
          <div class="ds-description-label">Filter definition</div>
          <code class="ds-description-text">{{ dataset.description }}</code>
        </div>
        <div v-if="sortedDatasetMeta.length" class="ds-meta-grid">
          <template v-for="[k, v] in sortedDatasetMeta" :key="k">
            <div class="ds-meta-key">{{ k }}</div>
            <div class="ds-meta-val">{{ fmtMetaValue(v) }}</div>
          </template>
        </div>
        <div v-else class="ds-meta-empty">No metadata fields set on this dataset.</div>
      </div>
    </details>

    <div class="grid">
      <!-- Filter rail -->
      <aside class="rail">
        <div class="rail-section">
          <div class="rail-label">Filters</div>
          <label class="field">
            <span class="field-label">Runs</span>
            <input
              v-model="draft.runs"
              class="field-input"
              placeholder="27731,27732"
            />
          </label>
          <label class="field">
            <span class="field-label">Run range</span>
            <input
              v-model="draft.runRange"
              class="field-input"
              placeholder="27000-28000"
            />
          </label>
          <label class="field">
            <span class="field-label">Namespace</span>
            <input
              v-model="draft.namespaceOverride"
              class="field-input"
              placeholder="hd-protodune-det-reco"
            />
          </label>
        </div>

        <div class="rail-section">
          <div class="rail-label">
            Metadata
            <button class="link" @click="addMetaRow">+ add</button>
          </div>
          <div v-for="(row, i) in draft.metaRows" :key="i" class="meta-row">
            <input
              v-model="row.key"
              class="field-input meta-key"
              placeholder="core.data_tier"
            />
            <span class="meta-eq">=</span>
            <input
              v-model="row.value"
              class="field-input meta-val"
              placeholder="full-reconstructed"
            />
            <button class="link link-x" @click="removeMetaRow(i)">×</button>
          </div>
          <div v-if="draft.metaRows.length === 0" class="rail-hint">
            None — click "+ add" for KEY=VALUE filters.
          </div>
        </div>

        <div class="rail-section">
          <label class="field-inline">
            <input type="checkbox" v-model="draft.withMetadata" />
            <span class="field-label-inline">Include metadata</span>
          </label>
          <div class="rail-hint">
            Adds run, events, and full metadata to each row. Slower for huge results.
          </div>
        </div>

        <div class="rail-actions">
          <button class="btn btn-primary" :disabled="loading" @click="applyFilters">
            Apply
          </button>
          <button class="btn" :disabled="loading" @click="clearFilters">
            Clear
          </button>
        </div>
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
          <div class="placeholder-body">No files match these filters.</div>
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
            <div class="table-head" :class="{ 'with-meta': applied.withMetadata }">
              <div class="th col-name">File</div>
              <div class="th col-run" v-if="applied.withMetadata">Run</div>
              <div class="th col-events" v-if="applied.withMetadata">Events</div>
              <div class="th col-size">Size</div>
              <div class="th col-created">Created</div>
            </div>
            <div
              v-for="row in data.rows"
              :key="row.did"
              class="tr"
              :class="{ 'with-meta': applied.withMetadata }"
              @click="openFile(row.did)"
            >
              <div class="td col-name">
                <span class="ns">{{ row.namespace }}:</span><span class="nm">{{ row.name }}</span>
              </div>
              <div class="td col-run" v-if="applied.withMetadata">{{ pickRun(row) }}</div>
              <div class="td col-events" v-if="applied.withMetadata">{{ fmtNum(pickEvents(row)) }}</div>
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
  grid-template-columns: 240px 1fr;
  gap: 24px;
}

/* Rail */
.rail {
  display: flex;
  flex-direction: column;
  gap: 18px;
}
.rail-section {
  display: flex;
  flex-direction: column;
  gap: 8px;
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
.rail-hint {
  font-size: 11.5px;
  color: var(--faint);
  font-style: italic;
}

.field { display: flex; flex-direction: column; gap: 3px; }
.field-label { font-size: 11px; color: var(--dim); }
.field-input {
  height: 28px;
  padding: 0 8px;
  border: 1px solid var(--rule);
  background: var(--page);
  border-radius: 6px;
  font-family: var(--font-mono);
  font-size: 12px;
  color: var(--body);
  outline: none;
}
.field-input:focus { border-color: var(--accent); }

.meta-row {
  display: flex;
  align-items: center;
  gap: 4px;
}
.meta-key { flex: 1; min-width: 0; }
.meta-val { flex: 1; min-width: 0; }
.meta-eq { color: var(--faint); font-family: var(--font-mono); font-size: 12px; }
.link {
  background: none;
  border: none;
  color: var(--accent-ink);
  font-size: 11px;
  font-weight: 600;
  cursor: pointer;
  padding: 0;
}
.link-x {
  font-size: 16px;
  color: var(--faint);
  padding: 0 4px;
  line-height: 1;
}
.link-x:hover { color: var(--bad); }

.field-inline { display: flex; align-items: center; gap: 8px; cursor: pointer; }
.field-label-inline { font-size: 12.5px; color: var(--body); }

.rail-actions { display: flex; gap: 6px; }

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
  grid-template-columns: 1fr 90px 110px;
  gap: 12px;
  padding: 8px 16px;
  align-items: center;
  min-width: 720px;
}
.table-head.with-meta, .tr.with-meta {
  grid-template-columns: 1fr 70px 80px 90px 110px;
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
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
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

/* Dataset metadata panel */
.ds-info {
  background: var(--page);
  border: 1px solid var(--rule);
  border-radius: 10px;
  margin-bottom: 16px;
}
.ds-info-summary {
  display: flex;
  align-items: baseline;
  gap: 12px;
  padding: 10px 14px;
  cursor: pointer;
  font-size: 12.5px;
  list-style: none;
}
.ds-info-summary::-webkit-details-marker { display: none; }
.ds-info-summary::before {
  content: '▸';
  color: var(--faint);
  font-size: 10px;
  margin-right: 2px;
  transition: transform 0.15s;
}
.ds-info[open] .ds-info-summary::before { transform: rotate(90deg); }
.ds-info-label {
  font-family: var(--font-sans);
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.6px;
  text-transform: uppercase;
  color: var(--faint);
}
.ds-info-stats {
  color: var(--dim);
  font-family: var(--font-mono);
  font-size: 11.5px;
}

.ds-info-body {
  padding: 4px 14px 14px;
  border-top: 1px solid var(--rule-soft);
}
.ds-description {
  margin-top: 10px;
  margin-bottom: 12px;
}
.ds-description-label {
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.4px;
  color: var(--faint);
  text-transform: uppercase;
  margin-bottom: 4px;
}
.ds-description-text {
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
.ds-meta-grid {
  display: grid;
  grid-template-columns: 200px 1fr;
  gap: 4px 16px;
}
.ds-meta-key {
  font-size: 11px;
  color: var(--dim);
}
.ds-meta-val {
  font-family: var(--font-mono);
  font-size: 11.5px;
  color: var(--ink);
  word-break: break-word;
}
.ds-meta-empty {
  font-size: 12px;
  color: var(--faint);
  font-style: italic;
  margin-top: 8px;
}
</style>
