<script setup>
import { computed, onMounted, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { getRun, getRunConditions, getDetectors } from '../api.js';

const route = useRoute();
const router = useRouter();

const runParam = computed(() => route.params.run);
const lookupInput = ref('');

const loading = ref(false);
const error = ref(null);
const data = ref(null);

const detectors = ref([]);
const detectorId = ref(route.query.detector || '');
const conditions = ref(null);
const conditionsLoading = ref(false);
const conditionsError = ref(null);

const selectedDetector = computed(
  () => detectors.value.find((d) => d.id === detectorId.value) || null,
);
const conditionsActive = computed(
  () => !!detectorId.value && !!selectedDetector.value?.condb_folder,
);

async function fetchRun() {
  if (!runParam.value) {
    data.value = null;
    return;
  }
  loading.value = true;
  error.value = null;
  data.value = null;
  try {
    data.value = await getRun(runParam.value);
  } catch (e) {
    error.value = e;
  } finally {
    loading.value = false;
  }
}

async function fetchConditions() {
  conditions.value = null;
  conditionsError.value = null;
  if (!runParam.value || !conditionsActive.value) return;
  conditionsLoading.value = true;
  try {
    conditions.value = await getRunConditions(detectorId.value, runParam.value);
  } catch (e) {
    if (e.status !== 404) conditionsError.value = e;
  } finally {
    conditionsLoading.value = false;
  }
}

function onDetectorChange() {
  router.replace({
    name: 'run-detail',
    params: { run: runParam.value },
    query: detectorId.value ? { detector: detectorId.value } : {},
  });
  fetchConditions();
}

function onLookup() {
  const trimmed = lookupInput.value.trim();
  if (!trimmed) return;
  router.push({
    name: 'run-detail',
    params: { run: trimmed },
    query: detectorId.value ? { detector: detectorId.value } : {},
  });
}

function openFile(did) {
  router.push({ name: 'file-detail', params: { did } });
}

function goToFilesQuery(tier) {
  // Cross-page link: jump into QueryView pre-populated.
  const base = `files where core.runs in (${runParam.value})`;
  const mql = tier
    ? `${base} and core.data_tier = '${tier.replace(/'/g, "\\'")}'`
    : base;
  router.push({ name: 'query', query: { prefill: mql } });
}

watch(runParam, () => {
  // Fire both lookups in parallel. The conditions call typically returns
  // before metacat's file query.
  fetchRun();
  fetchConditions();
});
onMounted(async () => {
  try {
    detectors.value = await getDetectors();
  } catch {
    detectors.value = [];
  }
  fetchRun();
  fetchConditions();
});

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
function fmtUtc(ts) {
  if (ts == null) return '—';
  return new Date(ts * 1000).toISOString().replace('T', ' ').slice(0, 19) + ' UTC';
}
function fmtDuration(sec) {
  if (sec == null) return '—';
  if (sec < 60) return `${sec.toFixed(1)}s`;
  const m = sec / 60;
  if (m < 60) return `${m.toFixed(1)} min`;
  const h = m / 60;
  return `${h.toFixed(2)} h`;
}
function tierBarWidth(count, total) {
  return Math.max(2, (count / total) * 100) + '%';
}

function fmtMom(v) {
  if (v == null) return '—';
  return `${v.toFixed(3)} GeV/c`;
}
function fmtHv(v) {
  if (v == null) return '—';
  return `${v.toLocaleString(undefined, { maximumFractionDigits: 0 })} V`;
}

const condRow = computed(() => conditions.value?.row || null);
const condBeam = computed(() => {
  const r = condRow.value;
  if (!r) return null;
  return r.beam_momentum ?? r.beam_momentum_mean ?? null;
});
const condHv = computed(() => {
  const r = condRow.value;
  if (!r) return null;
  return r.detector_hv ?? r.detector_hv_mean ?? null;
});
const condDuration = computed(() => {
  const r = condRow.value;
  if (!r || r.start_time == null || r.stop_time == null) return null;
  return r.stop_time - r.start_time;
});

// Fields rendered in the curated kv-grid above — exclude from "show all".
const CURATED = new Set([
  'start_time', 'stop_time', 'run_type', 'data_stream',
  'beam_momentum', 'beam_momentum_mean', 'beam_polarity',
  'detector_hv', 'detector_hv_mean', 'software_version',
  'config_files',
]);
const condExtras = computed(() => {
  const r = condRow.value;
  if (!r) return [];
  return Object.entries(r)
    .filter(([k, v]) => !CURATED.has(k) && v !== null && v !== '')
    .sort(([a], [b]) => a.localeCompare(b));
});
function fmtCellValue(v) {
  if (v === null || v === undefined) return '—';
  if (typeof v === 'number') {
    // Keep integers integer, floats to ≤ 6 sig figs
    return Number.isInteger(v) ? String(v) : Number(v.toPrecision(6)).toString();
  }
  if (typeof v === 'boolean') return v ? 'true' : 'false';
  return String(v);
}
</script>

<template>
  <div class="page">
    <div class="header">
      <div class="eyebrow">Run</div>
      <h1 class="title">
        <template v-if="runParam">Run {{ runParam }}</template>
        <template v-else>Look up a run</template>
      </h1>
      <p class="subtitle" v-if="data">
        {{ fmtDuration(data.duration_seconds) }} · {{ fmtNum(data.files_total) }} files
        <template v-if="data.start_time">
          · started {{ fmtUtc(data.start_time) }}
        </template>
      </p>
      <p class="subtitle" v-else-if="!runParam">
        Enter a run number to see when it ran and what files came out of it.
      </p>
    </div>

    <!-- Lookup form: always present, on the right when a run is shown -->
    <div class="lookup-bar">
      <input
        v-model="lookupInput"
        type="text"
        inputmode="numeric"
        class="lookup-input"
        placeholder="run number"
        @keyup.enter="onLookup"
      />
      <button class="btn btn-primary" :disabled="!lookupInput.trim()" @click="onLookup">
        Look up
      </button>
      <select
        v-model="detectorId"
        class="detector-picker"
        @change="onDetectorChange"
        :title="'Detector — picks which condb folder to query'"
      >
        <option value="">Unknown detector</option>
        <option v-for="d in detectors" :key="d.id" :value="d.id">
          {{ d.name }}{{ d.condb_folder ? '' : ' (no condb)' }}
        </option>
      </select>
    </div>

    <!-- Conditions card: independent of metacat — renders as soon as condb answers. -->
    <section
      v-if="runParam && (conditionsActive || conditionsLoading)"
      class="card cond-card"
    >
      <div class="card-head">
        Run conditions (condb)
        <span v-if="conditionsLoading" class="card-head-action">loading…</span>
      </div>
      <div v-if="conditionsError" class="card-empty">
        Couldn't load conditions: {{ conditionsError.message }}
      </div>
      <div v-else-if="conditions === null && !conditionsLoading" class="card-empty">
        No conditions on file for run {{ runParam }} in this detector's condb folder.
      </div>
      <div v-else-if="condRow" class="kv-grid kv-grid-cond">
        <div class="kv-key">Start</div>
        <div class="kv-val mono">{{ fmtUtc(condRow.start_time) }}</div>
        <div class="kv-key">Stop</div>
        <div class="kv-val mono">{{ fmtUtc(condRow.stop_time) }}</div>
        <div class="kv-key">Duration</div>
        <div class="kv-val mono">{{ fmtDuration(condDuration) }}</div>
        <div class="kv-key">Run type</div>
        <div class="kv-val">{{ condRow.run_type || '—' }}</div>
        <div class="kv-key">Stream</div>
        <div class="kv-val">{{ condRow.data_stream || '—' }}</div>
        <div class="kv-key">Beam</div>
        <div class="kv-val mono">
          {{ fmtMom(condBeam) }}
          <span v-if="condRow.beam_polarity" class="dim">
            ({{ condRow.beam_polarity }})
          </span>
        </div>
        <div class="kv-key">Detector HV</div>
        <div class="kv-val mono">{{ fmtHv(condHv) }}</div>
        <div class="kv-key">Software</div>
        <div class="kv-val mono">{{ condRow.software_version || '—' }}</div>
      </div>
      <details v-if="condRow?.config_files" class="cond-config">
        <summary>Config files ({{ Object.keys(condRow.config_files).length }})</summary>
        <ul class="config-list">
          <li v-for="(uri, key) in condRow.config_files" :key="key">
            <span class="config-key">{{ key }}</span>
            <span class="config-uri mono">{{ uri }}</span>
          </li>
        </ul>
      </details>
      <details v-if="condExtras.length" class="cond-config">
        <summary>All fields ({{ condExtras.length }})</summary>
        <ul class="config-list">
          <li v-for="[k, v] in condExtras" :key="k">
            <span class="config-key">{{ k }}</span>
            <span class="config-uri mono">{{ fmtCellValue(v) }}</span>
          </li>
        </ul>
      </details>
    </section>
    <section
      v-else-if="runParam && detectorId && !selectedDetector?.condb_folder"
      class="card cond-card"
    >
      <div class="card-head">Run conditions (condb)</div>
      <div class="card-empty">
        No condb integration configured for
        {{ selectedDetector?.name || detectorId }}.
      </div>
    </section>

    <div v-if="!runParam" class="empty">
      <div class="empty-body">
        Type a run number above and press Enter, or click into the
        <RouterLink :to="{ name: 'datasets' }" class="empty-link">Datasets</RouterLink>
        view to browse first.
      </div>
    </div>

    <div v-else-if="loading" class="placeholder">
      <div class="placeholder-body">Looking up run {{ runParam }}…</div>
    </div>

    <div v-else-if="error?.status === 404" class="placeholder">
      <div class="placeholder-title">No files for run {{ runParam }}</div>
      <div class="placeholder-body">Check the number and try again.</div>
    </div>

    <div v-else-if="error" class="error-card">
      <div class="error-title">Couldn't load run {{ runParam }}</div>
      <div class="error-detail">{{ error.message }}</div>
    </div>

    <div v-else-if="data" class="grid">
      <!-- Run window -->
      <section class="card">
        <div class="card-head">Run window (from raw files)</div>
        <div class="kv-grid">
          <div class="kv-key">Start</div>
          <div class="kv-val mono">{{ fmtUtc(data.start_time) }}</div>
          <div class="kv-key">End</div>
          <div class="kv-val mono">{{ fmtUtc(data.end_time) }}</div>
          <div class="kv-key">Duration</div>
          <div class="kv-val mono">{{ fmtDuration(data.duration_seconds) }}</div>
        </div>
        <p
          v-if="!data.start_time"
          class="card-foot-note"
        >
          No raw files for this run — start/end can't be inferred.
        </p>
      </section>

      <!-- Files by tier -->
      <section class="card">
        <div class="card-head">
          Files by tier · {{ fmtNum(data.files_total) }} total
        </div>
        <ul class="tier-list">
          <li
            v-for="(count, tier) in data.files_by_tier"
            :key="tier"
            class="tier-row"
            @click="goToFilesQuery(tier)"
            :title="`Open in Query: files where core.runs in (${runParam}) and core.data_tier = '${tier}'`"
          >
            <span class="tier-name">{{ tier }}</span>
            <span class="tier-count">{{ fmtNum(count) }}</span>
            <span class="tier-bar">
              <span class="tier-bar-fill" :style="{ width: tierBarWidth(count, data.files_total) }" />
            </span>
            <span class="tier-arrow">→</span>
          </li>
        </ul>
      </section>

      <!-- Sample raw files -->
      <section class="card card-wide">
        <div class="card-head">
          Sample raw files
          <span class="card-head-action" @click="goToFilesQuery(null)">
            Open in Query →
          </span>
        </div>
        <ul class="file-list">
          <li
            v-for="f in data.sample_raw_files"
            :key="f.did"
            class="file-row"
            @click="openFile(f.did)"
          >
            <span class="file-name">{{ f.name }}</span>
            <span class="file-size">{{ fmtBytes(f.size) }}</span>
          </li>
        </ul>
        <div v-if="data.sample_raw_files.length === 0" class="card-empty">
          No raw files for this run.
        </div>
      </section>
    </div>
  </div>
</template>

<style scoped>
.page { padding: 22px 36px 28px; }

.header {
  padding-bottom: 16px;
  border-bottom: 1px solid var(--rule);
  margin-bottom: 20px;
  position: relative;
}
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

/* Lookup bar */
.lookup-bar {
  display: flex;
  gap: 8px;
  margin-bottom: 20px;
  max-width: 520px;
  align-items: center;
}
.detector-picker {
  height: 32px;
  padding: 0 10px;
  border: 1px solid var(--rule);
  background: var(--page);
  border-radius: 8px;
  font-family: var(--font-sans);
  font-size: 13px;
  color: var(--body);
  outline: none;
  cursor: pointer;
}
.detector-picker:focus { border-color: var(--accent); }
.lookup-input {
  flex: 1;
  height: 32px;
  padding: 0 12px;
  border: 1px solid var(--rule);
  background: var(--page);
  border-radius: 8px;
  font-family: var(--font-mono);
  font-size: 13px;
  color: var(--body);
  outline: none;
}
.lookup-input:focus { border-color: var(--accent); }

/* Buttons */
.btn {
  display: inline-flex;
  align-items: center;
  height: 32px;
  padding: 0 14px;
  border: 1px solid var(--rule);
  background: var(--page);
  color: var(--body);
  font-family: var(--font-sans);
  font-size: 13px;
  font-weight: 500;
  border-radius: 7px;
  cursor: pointer;
  transition: background 0.12s;
}
.btn:hover:not(:disabled) { background: var(--surface); }
.btn:disabled { opacity: 0.5; cursor: not-allowed; }
.btn-primary {
  background: var(--ink);
  border-color: var(--ink);
  color: white;
}
.btn-primary:hover:not(:disabled) { background: var(--ink); }

/* Grid */
.grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
  align-items: start;
}
.card-wide { grid-column: 1 / -1; }

.card {
  background: var(--page);
  border: 1px solid var(--rule);
  border-radius: 10px;
  overflow: hidden;
}
.card-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 10px 14px;
  background: var(--surface);
  border-bottom: 1px solid var(--rule);
  font-family: var(--font-sans);
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 1.0px;
  text-transform: uppercase;
  color: var(--faint);
}
.card-head-action {
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0;
  text-transform: none;
  color: var(--accent-ink);
  cursor: pointer;
}
.card-head-action:hover { text-decoration: underline; }
.card-foot-note {
  padding: 0 14px 12px;
  margin: 6px 0 0;
  font-size: 11.5px;
  color: var(--faint);
  font-style: italic;
}
.card-empty {
  padding: 16px 14px;
  font-size: 12.5px;
  color: var(--faint);
  font-style: italic;
}

.kv-grid {
  display: grid;
  grid-template-columns: 80px 1fr;
  gap: 8px 12px;
  padding: 12px 14px;
}
.kv-key { font-size: 11.5px; color: var(--dim); }
.kv-val { font-size: 12.5px; color: var(--ink); }
.mono { font-family: var(--font-mono); }

/* Tier bars */
.tier-list { list-style: none; margin: 0; padding: 8px 14px 12px; }
.tier-row {
  display: grid;
  grid-template-columns: 160px 60px 1fr 16px;
  gap: 12px;
  align-items: center;
  padding: 6px 8px;
  margin: 0 -8px;
  border-radius: 6px;
  cursor: pointer;
  transition: background 0.12s;
}
.tier-row:hover { background: var(--surface); }
.tier-row:hover .tier-arrow { color: var(--accent-ink); opacity: 1; }
.tier-arrow {
  color: var(--faint);
  font-size: 13px;
  opacity: 0;
  transition: opacity 0.12s;
}
.tier-name {
  font-family: var(--font-mono);
  font-size: 11.5px;
  color: var(--ink);
  word-break: break-word;
}
.tier-count {
  font-family: var(--font-mono);
  font-size: 11.5px;
  color: var(--dim);
  text-align: right;
}
.tier-bar {
  display: block;
  height: 6px;
  background: var(--surface-2);
  border-radius: 999px;
  overflow: hidden;
}
.tier-bar-fill {
  display: block;
  height: 100%;
  background: var(--accent);
}

/* File list */
.file-list { list-style: none; margin: 0; padding: 4px 0; }
.file-row {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 12px;
  padding: 6px 14px;
  cursor: pointer;
  transition: background 0.12s;
}
.file-row:hover { background: var(--surface); }
.file-name {
  font-family: var(--font-mono);
  font-size: 11.5px;
  color: var(--ink);
  word-break: break-all;
  flex: 1;
}
.file-size {
  font-family: var(--font-mono);
  font-size: 11px;
  color: var(--faint);
  flex-shrink: 0;
}

/* Placeholders */
.empty, .placeholder {
  padding: 28px;
  background: var(--page);
  border: 1px solid var(--rule);
  border-radius: 10px;
  text-align: center;
}
.empty-body { font-size: 13.5px; color: var(--dim); }
.empty-link { color: var(--accent-ink); text-decoration: none; font-weight: 500; }
.empty-link:hover { text-decoration: underline; }
.placeholder-title {
  font-size: 14px;
  font-weight: 600;
  color: var(--ink);
  margin-bottom: 6px;
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

/* Conditions card */
.cond-card { margin-bottom: 16px; }
.kv-grid-cond { grid-template-columns: 110px 1fr; }
.dim { color: var(--faint); }

.cond-config { padding: 6px 14px 14px; }
.cond-config summary {
  cursor: pointer;
  font-size: 11.5px;
  color: var(--dim);
  font-family: var(--font-sans);
  padding: 4px 0;
}
.cond-config summary:hover { color: var(--ink); }
.config-list {
  list-style: none;
  margin: 6px 0 0;
  padding: 8px;
  background: var(--surface);
  border-radius: 6px;
  font-size: 11.5px;
}
.config-list li {
  display: flex;
  gap: 8px;
  padding: 3px 0;
  align-items: baseline;
}
.config-key {
  font-family: var(--font-mono);
  color: var(--dim);
  min-width: 90px;
  flex-shrink: 0;
}
.config-uri {
  font-size: 11px;
  color: var(--ink);
  word-break: break-all;
}
</style>
