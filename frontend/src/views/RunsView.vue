<script setup>
import { computed, onMounted, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { getDetectors, getRunsConditions } from '../api.js';

const route = useRoute();
const router = useRouter();

const detectors = ref([]);
const detectorId = ref(route.query.detector || '');
const runMin = ref(route.query.run_min || '');
const runMax = ref(route.query.run_max || '');
const startDate = ref(route.query.start || '');
const stopDate = ref(route.query.stop || '');
const beamMin = ref(route.query.beam_setp_min || '');
const beamMax = ref(route.query.beam_setp_max || '');
// 'any' = no polarity filter. Server-side translation: 'positive' or 'Negative'.
const polarity = ref(route.query.polarity || 'any');
// PROD is the default — TEST runs are usually noise. 'ALL' means no filter.
const runType = ref(route.query.run_type || 'PROD');

const rows = ref([]);
const loading = ref(false);
const error = ref(null);
const submitted = ref(false);

const condbDetectors = computed(() =>
  detectors.value.filter((d) => d.condb_folder),
);
const selectedDetector = computed(
  () => detectors.value.find((d) => d.id === detectorId.value) || null,
);
const hasRunRange = computed(() => runMin.value !== '' && runMax.value !== '');
const hasDateRange = computed(() => startDate.value !== '' || stopDate.value !== '');
const hasBeamFilter = computed(
  () =>
    beamMin.value !== '' ||
    beamMax.value !== '' ||
    (polarity.value && polarity.value !== 'any'),
);
const canApply = computed(() => {
  if (!selectedDetector.value?.condb_folder) return false;
  if (!hasRunRange.value && !hasDateRange.value && !hasBeamFilter.value)
    return false;
  if (hasRunRange.value && Number(runMin.value) > Number(runMax.value)) return false;
  if (
    startDate.value !== '' &&
    stopDate.value !== '' &&
    stopDate.value < startDate.value
  )
    return false;
  if (
    beamMin.value !== '' &&
    beamMax.value !== '' &&
    Number(beamMin.value) > Number(beamMax.value)
  )
    return false;
  return true;
});

async function fetchRows() {
  if (!canApply.value) return;
  loading.value = true;
  error.value = null;
  rows.value = [];
  submitted.value = true;
  try {
    const payload = await getRunsConditions(detectorId.value, {
      run_min: runMin.value,
      run_max: runMax.value,
      start: startDate.value,
      stop: stopDate.value,
      run_type: runType.value,
      beam_setp_min: beamMin.value,
      beam_setp_max: beamMax.value,
      polarity: polarity.value,
    });
    rows.value = payload.rows || [];
  } catch (e) {
    error.value = e;
  } finally {
    loading.value = false;
  }
}

function onApply() {
  router.replace({
    name: 'runs-search',
    query: {
      detector: detectorId.value,
      run_min: runMin.value,
      run_max: runMax.value,
      start: startDate.value,
      stop: stopDate.value,
      run_type: runType.value,
      beam_setp_min: beamMin.value,
      beam_setp_max: beamMax.value,
      polarity: polarity.value,
    },
  });
  fetchRows();
}

function openRun(tv) {
  router.push({
    name: 'run-detail',
    params: { run: String(Math.trunc(tv)) },
    query: { detector: detectorId.value },
  });
}

onMounted(async () => {
  try {
    detectors.value = await getDetectors();
  } catch {
    detectors.value = [];
  }
  // Default to the first condb-enabled detector if none in URL.
  if (!detectorId.value && condbDetectors.value.length) {
    detectorId.value = condbDetectors.value[0].id;
  }
  // Auto-fetch if the URL has a full query.
  if (canApply.value) fetchRows();
});

watch(detectorId, () => {
  // Switching detector invalidates results until Apply is pressed again.
  rows.value = [];
  submitted.value = false;
});

function fmtUtc(ts) {
  if (ts == null) return '—';
  return new Date(ts * 1000).toISOString().replace('T', ' ').slice(0, 19);
}
function fmtMom(v) {
  if (v == null) return '—';
  return v.toFixed(3);
}
function fmtNum1(v) {
  if (v == null) return '—';
  return Number(v).toFixed(1);
}
function fmtInt(v) {
  if (v == null) return '—';
  return String(Math.trunc(Number(v)));
}
function beamOf(r) {
  return r.beam_momentum ?? r.beam_momentum_mean ?? null;
}
function beamSetOf(r) {
  // HD: beam_setmomentum. VD: beam_momentum_set.
  return r.beam_setmomentum ?? r.beam_momentum_set ?? null;
}
</script>

<template>
  <div class="page">
    <div class="header">
      <div class="eyebrow">Runs</div>
      <h1 class="title">Run search</h1>
      <p class="subtitle">
        Browse condb run-condition rows. Pick a detector, narrow by run-number
        range, click a row for full details.
      </p>
    </div>

    <div class="filters">
      <label class="field">
        <span class="field-label">Detector</span>
        <select v-model="detectorId" class="control">
          <option value="" disabled>— choose a detector —</option>
          <option v-for="d in condbDetectors" :key="d.id" :value="d.id">
            {{ d.name }}
          </option>
        </select>
      </label>
      <label class="field">
        <span class="field-label">Type</span>
        <select v-model="runType" class="control">
          <option value="PROD">PROD only</option>
          <option value="TEST">TEST only</option>
          <option value="ALL">All</option>
        </select>
      </label>
      <label class="field">
        <span class="field-label">Run min</span>
        <input
          v-model="runMin"
          type="number"
          inputmode="numeric"
          class="control mono"
          placeholder="e.g. 27298"
        />
      </label>
      <label class="field">
        <span class="field-label">Run max</span>
        <input
          v-model="runMax"
          type="number"
          inputmode="numeric"
          class="control mono"
          placeholder="e.g. 27310"
          @keyup.enter="canApply && onApply()"
        />
      </label>
      <label class="field">
        <span class="field-label">Start date (UTC)</span>
        <input
          v-model="startDate"
          type="date"
          class="control mono"
          @keyup.enter="canApply && onApply()"
        />
      </label>
      <label class="field">
        <span class="field-label">Stop date (UTC)</span>
        <input
          v-model="stopDate"
          type="date"
          class="control mono"
          @keyup.enter="canApply && onApply()"
        />
      </label>
      <label class="field">
        <span class="field-label">|Beam setp| min</span>
        <input
          v-model="beamMin"
          type="number"
          step="any"
          min="0"
          class="control mono"
          placeholder="GeV/c"
        />
      </label>
      <label class="field">
        <span class="field-label">|Beam setp| max</span>
        <input
          v-model="beamMax"
          type="number"
          step="any"
          min="0"
          class="control mono"
          placeholder="GeV/c"
          @keyup.enter="canApply && onApply()"
        />
      </label>
      <label class="field">
        <span class="field-label">Polarity</span>
        <select v-model="polarity" class="control">
          <option value="any">Any</option>
          <option value="positive">Positive</option>
          <option value="negative">Negative</option>
        </select>
      </label>
      <button class="btn btn-primary" :disabled="!canApply" @click="onApply">
        Apply
      </button>
    </div>
    <p class="hint">
      Need at least one filter: run range, date range, or beam.
      Dates are inclusive UTC days. Beam bounds are magnitudes (|setp|);
      polarity selects the sign — "Any" with bounds unions positive and
      negative sides.
    </p>

    <div v-if="loading" class="placeholder">
      <div class="placeholder-body">Loading conditions…</div>
    </div>

    <div v-else-if="error" class="error-card">
      <div class="error-title">Couldn't load conditions</div>
      <div class="error-detail">{{ error.message }}</div>
    </div>

    <div v-else-if="submitted && rows.length === 0" class="placeholder">
      <div class="placeholder-title">No rows in this range</div>
      <div class="placeholder-body">
        Try a different run range, or pick another detector. The condb folder
        may not have entries for every run number.
      </div>
    </div>

    <div v-else-if="rows.length" class="results-card">
      <div class="card-head">
        {{ rows.length }} {{ rows.length === 1 ? 'row' : 'rows' }}
        · {{ selectedDetector?.name }}
      </div>
      <table class="results-table">
        <thead>
          <tr>
            <th>run</th>
            <th>start (UTC)</th>
            <th>type</th>
            <th>stream</th>
            <th title="Beam setpoint (filter value)">beam (set)</th>
            <th title="Beam measured (mean); can diverge from setpoint">beam (meas)</th>
            <th>polarity</th>
            <th title="gain (mV/fC)">gain</th>
            <th title="peak_time (µs)">peak</th>
            <th title="leak current (HD only)">leak</th>
            <th title="baseline (ADC code)">base</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="r in rows"
            :key="r.tv"
            class="row"
            @click="openRun(r.tv)"
          >
            <td class="mono">{{ Math.trunc(r.tv) }}</td>
            <td class="mono dim">{{ fmtUtc(r.start_time) }}</td>
            <td>{{ r.run_type || '—' }}</td>
            <td>{{ r.data_stream || '—' }}</td>
            <td class="mono">{{ fmtMom(beamSetOf(r)) }}</td>
            <td class="mono dim">{{ fmtMom(beamOf(r)) }}</td>
            <td>{{ r.beam_polarity || '—' }}</td>
            <td class="mono">{{ fmtNum1(r.gain) }}</td>
            <td class="mono">{{ fmtNum1(r.peak_time) }}</td>
            <td class="mono">{{ fmtNum1(r.leak) }}</td>
            <td class="mono">{{ fmtInt(r.baseline) }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-else class="placeholder">
      <div class="placeholder-body">
        Set a detector and run range above, then click Apply.
      </div>
    </div>
  </div>
</template>

<style scoped>
.page { padding: 22px 36px 28px; }
.header { padding-bottom: 16px; border-bottom: 1px solid var(--rule); margin-bottom: 20px; }
.eyebrow { font-family: var(--font-mono); font-size: 11.5px; color: var(--faint); margin-bottom: 4px; }
.title { font-size: 22px; font-weight: 600; letter-spacing: -0.4px; margin: 0 0 4px; color: var(--ink); }
.subtitle { font-size: 13.5px; color: var(--dim); margin: 0; max-width: 680px; }

.filters {
  display: flex;
  gap: 14px;
  align-items: flex-end;
  margin-bottom: 20px;
  flex-wrap: wrap;
}
.field { display: flex; flex-direction: column; gap: 4px; }
.field-label {
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.8px;
  color: var(--faint);
}
.control {
  height: 32px;
  padding: 0 10px;
  border: 1px solid var(--rule);
  background: var(--page);
  border-radius: 8px;
  font-family: var(--font-sans);
  font-size: 13px;
  color: var(--body);
  outline: none;
}
.control.mono { font-family: var(--font-mono); width: 130px; }
.control:focus { border-color: var(--accent); }

.hint {
  margin: -8px 0 18px;
  font-size: 11.5px;
  color: var(--faint);
  font-style: italic;
}

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
}
.btn:disabled { opacity: 0.5; cursor: not-allowed; }
.btn-primary {
  background: var(--ink);
  border-color: var(--ink);
  color: white;
}

.placeholder {
  padding: 28px;
  background: var(--page);
  border: 1px solid var(--rule);
  border-radius: 10px;
  text-align: center;
}
.placeholder-title { font-size: 14px; font-weight: 600; color: var(--ink); margin-bottom: 6px; }
.placeholder-body { font-size: 13.5px; color: var(--dim); }

.error-card {
  padding: 14px 16px;
  background: var(--bad-bg);
  border: 1px solid var(--bad);
  border-radius: 10px;
}
.error-title { font-weight: 600; color: var(--bad); margin-bottom: 4px; }
.error-detail { font-family: var(--font-mono); font-size: 12px; color: var(--ink); }

.results-card {
  background: var(--page);
  border: 1px solid var(--rule);
  border-radius: 10px;
  overflow: hidden;
}
.card-head {
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

.results-table {
  width: 100%;
  border-collapse: collapse;
}
.results-table th,
.results-table td {
  padding: 6px 14px;
  text-align: left;
  font-size: 12.5px;
  color: var(--ink);
  border-bottom: 1px solid var(--rule);
}
.results-table th {
  font-size: 10.5px;
  font-weight: 600;
  letter-spacing: 0.8px;
  text-transform: uppercase;
  color: var(--faint);
  background: var(--surface);
}
.results-table tbody .row { cursor: pointer; transition: background 0.12s; }
.results-table tbody .row:hover { background: var(--surface); }
.mono { font-family: var(--font-mono); font-size: 11.5px; }
.dim { color: var(--dim); }
</style>
