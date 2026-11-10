<script setup>
import { computed, onMounted, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { getDataset, getFiles, getFilesCount, getRules } from '../api.js';
import { loadDetectors, nav } from '../composables/useNav.js';
import { useFileIdCopy } from '../composables/useFileIdCopy.js';
import FileTable from '../components/FileTable.vue';

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

// Rucio replication rules: looked up on demand, never cached.
const rules = ref(null);            // array of rule rows once loaded
const rulesLoading = ref(false);
const rulesError = ref(null);
const rulesOpen = ref(false);

async function toggleRules() {
  if (rulesOpen.value) {
    rulesOpen.value = false;
    return;
  }
  rulesOpen.value = true;
  if (rules.value || rulesLoading.value) return;
  const forDid = did.value;
  rulesLoading.value = true;
  rulesError.value = null;
  try {
    const r = await getRules(forDid);
    if (forDid === did.value) rules.value = r.rules;
  } catch (e) {
    if (forDid === did.value) rulesError.value = e;
  } finally {
    rulesLoading.value = false;
  }
}

watch(did, () => {
  rules.value = null;
  rulesError.value = null;
  rulesOpen.value = false;
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

// DIDs the user has explicitly ticked. Persisted in sessionStorage keyed
// by (dataset, page, pageSize) so the selection survives a remount when
// the user clicks a row by accident and hits the browser back button.
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
  } catch (_e) { /* sessionStorage full or unavailable — fine */ }
}, { deep: true });

const fileRows = computed(() => data.value?.rows || []);
const { copyState, copyCount, copyDids, selectableDids } = useFileIdCopy(
  fileRows,
  selected,
);

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

const sortedDatasetMeta = computed(() => {
  if (!dataset.value?.metadata) return [];
  return Object.entries(dataset.value.metadata).sort(([a], [b]) => a.localeCompare(b));
});

function fmtTimestampLong(ts) {
  if (ts == null) return '—';
  const d = typeof ts === 'number' ? new Date(ts * 1000) : new Date(ts);
  return d.toISOString().slice(0, 16).replace('T', ' ') + ' UTC';
}
function fmtBytes(n) {
  if (n == null) return '—';
  const units = ['B', 'kB', 'MB', 'GB', 'TB', 'PB'];
  let i = 0;
  let v = n;
  while (v >= 1000 && i < units.length - 1) {
    v /= 1000;
    i += 1;
  }
  return `${i === 0 ? v : v.toFixed(2)} ${units[i]}`;
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
        <div class="card">
          <div class="card-head">
            <span>
              Rucio rules
              <span v-if="rules" class="head-meta">· {{ rules.length }}</span>
            </span>
            <button class="btn btn-small" @click="toggleRules">
              {{ rulesOpen ? 'Hide' : (rules ? 'Show' : 'Look up') }}
            </button>
          </div>
          <template v-if="rulesOpen">
            <div v-if="rulesLoading" class="card-empty">Looking up rules…</div>
            <div v-else-if="rulesError" class="rules-error">
              <div class="error-title">
                {{ rulesError.status === 401 ? 'Rucio auth' : 'Rucio error' }}
              </div>
              <div class="error-detail">{{ rulesError.message }}</div>
            </div>
            <div v-else-if="rules?.length" class="rules-scroll">
              <table class="rules-table">
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>Account</th>
                    <th title="Rule state, with file locks OK / replicating / stuck">
                      State [OK/REPL/STUCK]
                    </th>
                    <th>RSE expression</th>
                    <th class="num">Copies</th>
                    <th class="num">Size</th>
                    <th>Expires (UTC)</th>
                    <th>Created (UTC)</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="r in rules" :key="r.id">
                    <td>{{ r.id }}</td>
                    <td>{{ r.account }}</td>
                    <td>
                      <span class="rule-state" :class="`state-${(r.state || '').toLowerCase()}`">{{ r.state }}</span>
                      [{{ r.locks_ok_cnt }}/{{ r.locks_replicating_cnt }}/{{ r.locks_stuck_cnt }}]
                    </td>
                    <td>{{ r.rse_expression }}</td>
                    <td class="num">{{ r.copies }}</td>
                    <td class="num" :title="r.bytes != null ? `${fmtNum(r.bytes)} bytes` : ''">{{ fmtBytes(r.bytes) }}</td>
                    <td>{{ r.expires_at || '—' }}</td>
                    <td>{{ r.created_at || '—' }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
            <div v-else class="card-empty">No Rucio rules for this dataset.</div>
          </template>
        </div>

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

          <FileTable
            :rows="data.rows"
            v-model:selected="selected"
            :show-metadata="true"
            :metadata-loading="metadataLoading"
            empty-message="This dataset has no files."
            @open-file="openFile"
          />
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

.btn-small { height: 22px; padding: 0 8px; font-size: 11px; }

.main { display: flex; flex-direction: column; gap: 14px; min-width: 0; }

/* Rucio rules card */
.card {
  background: var(--page);
  border: 1px solid var(--rule);
  border-radius: 10px;
  overflow: hidden;
}
.card-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  padding: 8px 14px;
  background: var(--surface);
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 1.0px;
  text-transform: uppercase;
  color: var(--faint);
}
.head-meta {
  font-weight: 400;
  color: var(--dim);
  text-transform: none;
  letter-spacing: 0;
}
.card-empty {
  padding: 14px;
  border-top: 1px solid var(--rule);
  font-size: 12.5px;
  color: var(--faint);
  font-style: italic;
}
.rules-error {
  padding: 12px 14px;
  background: var(--bad-bg);
  border-top: 1px solid var(--bad);
}
.rules-scroll { overflow-x: auto; border-top: 1px solid var(--rule); }
.rules-table {
  width: 100%;
  border-collapse: collapse;
  font-family: var(--font-mono);
  font-size: 11.5px;
  color: var(--ink);
  white-space: nowrap;
}
.rules-table th {
  padding: 7px 14px;
  text-align: left;
  font-family: var(--font-sans);
  font-size: 11px;
  font-weight: 600;
  color: var(--dim);
  border-bottom: 1px solid var(--rule);
}
.rules-table td { padding: 7px 14px; border-top: 1px solid var(--rule); }
.rules-table tbody tr:first-child td { border-top: none; }
.rules-table .num { text-align: right; }
.rule-state { font-weight: 600; }
.rule-state.state-ok { color: var(--good); }
.rule-state.state-replicating { color: var(--warn); }
.rule-state.state-stuck { color: var(--bad); }

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
