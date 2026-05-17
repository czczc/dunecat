<script setup>
import { computed, onMounted, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { getCondbColumns, getDetectors, getRunsConditions } from '../api.js';
import { hasSelection } from '../composables/useRowNav.js';

const route = useRoute();
const router = useRouter();

const detectors = ref([]);
const detectorId = ref(route.query.detector || '');

// Single combined Run-spec input. Accepts:
//   "33"            single run
//   "2,3,9"         explicit list
//   "1:100"         inclusive range
//   "1:100:2"       stepped range
//   "100:1:-1"      reverse range
//   "1:10, 50, 60:65" arbitrary mix
// Legacy URL params run_min/run_max are migrated into the spec on load.
const runSpec = ref(
  route.query.runs ||
    (route.query.run_min && route.query.run_max
      ? `${route.query.run_min}:${route.query.run_max}`
      : ''),
);
const startDate = ref(route.query.start || '');
const stopDate = ref(route.query.stop || '');

// Single combined beam-spec input. Accepts a signed momentum range
// where the sign carries the polarity:
//   "3"        single value, +3 GeV/c, positive
//   "1:3"      1–3 GeV/c, positive
//   "3:1"      same range (order-insensitive)
//   "-1:-7"    1–7 GeV/c, negative
//   "-7:-1"    same range
// Mixed signs are an error. Empty = no beam filter.
const beamSpec = ref(
  route.query.beam ||
    _legacyBeamFromUrl(route.query.beam_setp_min, route.query.beam_setp_max, route.query.polarity),
);

function _legacyBeamFromUrl(min, max, pol) {
  if (!min && !max) return '';
  const sign = pol === 'negative' ? '-' : '';
  if (min && max) return `${sign}${min}:${sign}${max}`;
  return `${sign}${min || max}`;
}
// 'any' = no stream filter; else one of cosmics / calibration / physics.
const dataStream = ref(route.query.data_stream || 'any');
// PROD is the default — TEST runs are usually noise. 'ALL' means no filter.
const runType = ref(route.query.run_type || 'PROD');

const RUN_SPEC_HELP = [
  'Run spec syntax:',
  '  27305            single run',
  '  2,3,9            explicit list',
  '  1:100            inclusive range',
  '  1:100:2          stepped range',
  '  100:1:-1         reverse range',
  '  1:10, 50, 60:65  mix tokens with commas',
].join('\n');

const BEAM_SPEC_HELP = [
  'Beam range syntax (sign carries polarity):',
  '  3        single value, +3 GeV/c',
  '  1:3      1–3 GeV/c, positive polarity',
  '  3:1      same (order-insensitive)',
  '  -1:-7    1–7 GeV/c, negative polarity',
  '  -7:-1    same',
  'Mixed signs (e.g. -1:3) is rejected. Empty = no beam filter.',
].join('\n');

// Custom conditions — per-row {column, op, value} objects. URL persists
// each row as a `cond=COL OP VAL` string, parsed back on load.
const OPS = ['=', '!=', '<', '<=', '>', '>='];
const columns = ref([]);  // {name, type}[] for the current detector

function parseCondString(s) {
  const m = s.match(/^(\S+)\s+(\S+)\s+(.*)$/);
  if (!m) return null;
  return { column: m[1], op: m[2], value: m[3] };
}
function initialCustomConds() {
  const raw = route.query.cond;
  if (!raw) return [];
  const list = Array.isArray(raw) ? raw : [raw];
  return list.map(parseCondString).filter(Boolean);
}
const customConds = ref(initialCustomConds());

function colTypeOf(name) {
  return columns.value.find((c) => c.name === name)?.type || null;
}
function placeholderFor(type) {
  if (type === 'string') return "'value'  or  null";
  if (type === 'number') return '14.0';
  if (type === 'bool') return 'true / false';
  return 'value';
}
function buildCondString(row) {
  const col = row.column;
  const op = row.op;
  let val = (row.value ?? '').toString().trim();
  if (!col || !op || !val) return null;
  // Auto-quote string columns (most common gotcha) unless the user already
  // quoted or is filtering against null.
  const t = colTypeOf(col);
  if (t === 'string' && val !== 'null' && !/^['"]/.test(val)) {
    val = `'${val.replace(/'/g, '')}'`;
  }
  return `${col} ${op} ${val}`;
}
const builtCustomConds = computed(() =>
  customConds.value.map(buildCondString).filter(Boolean),
);
function addCondRow() {
  customConds.value.push({ column: '', op: '=', value: '' });
}
function removeCondRow(idx) {
  customConds.value.splice(idx, 1);
}

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
// Parse the spec into either {isRange: true, min, max} (sent as
// run_min/run_max) or {isRange: false, runs: [...]} (sent as runs=...).
// Empty / errored specs short-circuit hasRunRange to false.
const parsedRunSpec = computed(() => parseRunSpec(runSpec.value));
const hasRunRange = computed(
  () => !parsedRunSpec.value.isEmpty && !parsedRunSpec.value.error,
);

const parsedBeamSpec = computed(() => parseBeamSpec(beamSpec.value));
const hasBeamFilter = computed(
  () => !parsedBeamSpec.value.isEmpty && !parsedBeamSpec.value.error,
);

function parseBeamSpec(input) {
  const text = (input || '').trim();
  if (!text) return { isEmpty: true, error: null, min: null, max: null, polarity: 'any' };
  const m = text.match(/^(-?\d+(?:\.\d+)?)(?::(-?\d+(?:\.\d+)?))?$/);
  if (!m) return { isEmpty: false, error: `Invalid beam spec: '${text}'`, min: null, max: null, polarity: 'any' };
  const a = parseFloat(m[1]);
  const b = m[2] !== undefined ? parseFloat(m[2]) : a;
  const signA = Math.sign(a);
  const signB = Math.sign(b);
  if (signA !== 0 && signB !== 0 && signA !== signB) {
    return { isEmpty: false, error: 'Beam range must be same-sign (both + or both -)', min: null, max: null, polarity: 'any' };
  }
  const sign = signA || signB;  // 0 if both zero
  const polarity = sign < 0 ? 'negative' : 'positive';
  const min = Math.min(Math.abs(a), Math.abs(b));
  const max = Math.max(Math.abs(a), Math.abs(b));
  return { isEmpty: false, error: null, min, max, polarity };
}

function parseRunSpec(input) {
  const text = (input || '').trim();
  if (!text) {
    return { isEmpty: true, error: null, isRange: false, runs: null, min: null, max: null };
  }
  const tokens = text.split(',').map((t) => t.trim()).filter(Boolean);

  // Fast path: a single "A:B" token → simple inclusive range, sent as run_min/run_max.
  if (tokens.length === 1) {
    const m = tokens[0].match(/^(\d+):(\d+)$/);
    if (m) {
      const a = parseInt(m[1], 10);
      const b = parseInt(m[2], 10);
      if (a > b) return { isEmpty: false, error: `Range start > end (use 'A:B:-1' for reverse)`, isRange: false, runs: null, min: null, max: null };
      return { isEmpty: false, error: null, isRange: true, runs: null, min: a, max: b };
    }
  }

  // General path: expand every token to a set of runs.
  const set = new Set();
  for (const tok of tokens) {
    const m = tok.match(/^(\d+)(?::(\d+)(?::(-?\d+))?)?$/);
    if (!m) return { isEmpty: false, error: `Invalid token: '${tok}'`, isRange: false, runs: null, min: null, max: null };
    const a = parseInt(m[1], 10);
    if (m[2] === undefined) { set.add(a); continue; }
    const b = parseInt(m[2], 10);
    const step = m[3] === undefined ? (a <= b ? 1 : -1) : parseInt(m[3], 10);
    if (step === 0) return { isEmpty: false, error: `Step cannot be 0: '${tok}'`, isRange: false, runs: null, min: null, max: null };
    if (step > 0 && a > b) return { isEmpty: false, error: `Positive step but start > end: '${tok}'`, isRange: false, runs: null, min: null, max: null };
    if (step < 0 && a < b) return { isEmpty: false, error: `Negative step but start < end: '${tok}'`, isRange: false, runs: null, min: null, max: null };
    if (step > 0) {
      for (let v = a; v <= b; v += step) set.add(v);
    } else {
      for (let v = a; v >= b; v += step) set.add(v);
    }
    if (set.size > 5000) return { isEmpty: false, error: `Too many runs (>5000); use a plain range`, isRange: false, runs: null, min: null, max: null };
  }

  const runs = [...set].sort((x, y) => x - y);
  return {
    isEmpty: false, error: null, isRange: false, runs,
    min: runs[0] ?? null, max: runs[runs.length - 1] ?? null,
  };
}
const hasDateRange = computed(() => startDate.value !== '' || stopDate.value !== '');
const hasStreamFilter = computed(
  () => dataStream.value && dataStream.value !== 'any',
);
const hasCustomFilter = computed(() => builtCustomConds.value.length > 0);
const canApply = computed(() => {
  if (!selectedDetector.value?.condb_folder) return false;
  if (
    !hasRunRange.value &&
    !hasDateRange.value &&
    !hasBeamFilter.value &&
    !hasStreamFilter.value &&
    !hasCustomFilter.value
  )
    return false;
  if (parsedRunSpec.value.error) return false;
  if (parsedBeamSpec.value.error) return false;
  if (
    startDate.value !== '' &&
    stopDate.value !== '' &&
    stopDate.value < startDate.value
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
    const spec = parsedRunSpec.value;
    const runParams = spec.isRange
      ? { run_min: spec.min, run_max: spec.max }
      : spec.runs && spec.runs.length
        ? { runs: spec.runs }
        : {};
    const payload = await getRunsConditions(detectorId.value, {
      ...runParams,
      start: startDate.value,
      stop: stopDate.value,
      run_type: runType.value,
      data_stream: dataStream.value,
      beam_setp_min: hasBeamFilter.value ? parsedBeamSpec.value.min : '',
      beam_setp_max: hasBeamFilter.value ? parsedBeamSpec.value.max : '',
      polarity: hasBeamFilter.value ? parsedBeamSpec.value.polarity : 'any',
      cond: builtCustomConds.value,
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
      runs: runSpec.value || undefined,
      start: startDate.value,
      stop: stopDate.value,
      run_type: runType.value,
      data_stream: dataStream.value,
      beam: beamSpec.value || undefined,
      cond: builtCustomConds.value,
    },
  });
  fetchRows();
}

function openRun(tv) {
  if (hasSelection()) return;
  router.push({
    name: 'run-detail',
    params: { run: String(Math.trunc(tv)) },
    query: { detector: detectorId.value },
  });
}

async function loadColumns() {
  if (!detectorId.value) {
    columns.value = [];
    return;
  }
  try {
    columns.value = await getCondbColumns(detectorId.value);
  } catch {
    columns.value = [];
  }
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
  await loadColumns();
  // Auto-fetch if the URL has a full query.
  if (canApply.value) fetchRows();
});

watch(detectorId, async () => {
  // Switching detector invalidates results and reloads the column list
  // (each folder exposes a different curated column set).
  rows.value = [];
  submitted.value = false;
  await loadColumns();
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
      <h1 class="title">Run search</h1>
      <p class="subtitle">
        Browse condb
        <a
          href="https://wiki.dunescience.org/wiki/Run_Conditions_Table"
          target="_blank"
          rel="noopener noreferrer"
          class="ref-link"
        >run-condition ↗</a>
        rows. Pick a detector, narrow by run-number range, click a row for
        full details.
      </p>
    </div>

    <div class="filters">
      <label class="field">
        <span class="field-label">Detector</span>
        <select v-model="detectorId" class="control">
          <option v-for="d in condbDetectors" :key="d.id" :value="d.id">
            {{ d.name }}
          </option>
        </select>
      </label>
      <label class="field">
        <span class="field-label">Type</span>
        <select v-model="runType" class="control">
          <option value="PROD">PROD</option>
          <option value="TEST">TEST</option>
          <option value="ALL">ALL</option>
        </select>
      </label>
      <label class="field">
        <span class="field-label">Stream</span>
        <select v-model="dataStream" class="control">
          <option value="any">Any</option>
          <option value="cosmics">cosmics</option>
          <option value="calibration">calibration</option>
          <option value="physics">physics</option>
        </select>
      </label>
      <label class="field field-runs">
        <span class="field-label" :title="RUN_SPEC_HELP">
          Runs
          <span
            v-if="parsedRunSpec.error"
            class="field-hint err"
            :title="parsedRunSpec.error"
          >{{ parsedRunSpec.error }}</span>
          <span
            v-else-if="parsedRunSpec.isRange"
            class="field-hint"
          >range {{ parsedRunSpec.min }}–{{ parsedRunSpec.max }}</span>
          <span
            v-else-if="parsedRunSpec.runs && parsedRunSpec.runs.length"
            class="field-hint"
          >{{ parsedRunSpec.runs.length }} run{{ parsedRunSpec.runs.length === 1 ? '' : 's' }}</span>
        </span>
        <input
          v-model="runSpec"
          type="text"
          class="control mono"
          placeholder="e.g. 100:200, or 150, or 1:10:2"
          spellcheck="false"
          :title="RUN_SPEC_HELP"
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
      <label class="field field-beam">
        <span class="field-label" :title="BEAM_SPEC_HELP">
          Beam range (GeV/c)
          <span
            v-if="parsedBeamSpec.error"
            class="field-hint err"
            :title="parsedBeamSpec.error"
          >{{ parsedBeamSpec.error }}</span>
          <span
            v-else-if="!parsedBeamSpec.isEmpty"
            class="field-hint"
          >{{ parsedBeamSpec.polarity === 'negative' ? '−' : '+' }} {{ parsedBeamSpec.min }}–{{ parsedBeamSpec.max }}</span>
        </span>
        <input
          v-model="beamSpec"
          type="text"
          class="control mono"
          placeholder="e.g. 1:3, -7:-1, or 3"
          spellcheck="false"
          :title="BEAM_SPEC_HELP"
          @keyup.enter="canApply && onApply()"
        />
      </label>
      <button class="btn btn-primary" :disabled="!canApply" @click="onApply">
        Apply
      </button>
    </div>
    <p class="hint">
      Need at least one filter: runs, date range, beam, stream, or a
      custom condition below. Dates are inclusive UTC days. Beam range
      uses signed momentum (e.g. <code>1:3</code> = positive,
      <code>-7:-1</code> = negative).
    </p>

    <section class="custom-card" v-if="columns.length">
      <div class="custom-head">
        <span class="custom-title">Custom conditions</span>
        <button class="btn btn-small" @click="addCondRow">+ Add condition</button>
      </div>
      <div v-if="!customConds.length" class="custom-empty">
        ANDed with the filters above. Click + to add a column predicate
        (e.g. <code>gain &gt;= 14</code>, <code>software_version = 'fddaq-v4.4.6-a9-1'</code>).
      </div>
      <ul v-else class="cond-list">
        <li v-for="(row, i) in customConds" :key="i" class="cond-row">
          <select v-model="row.column" class="control cond-col">
            <option value="" disabled>— column —</option>
            <option v-for="c in columns" :key="c.name" :value="c.name">
              {{ c.name }} <span>({{ c.type }})</span>
            </option>
          </select>
          <select v-model="row.op" class="control cond-op">
            <option v-for="op in OPS" :key="op" :value="op">{{ op }}</option>
          </select>
          <input
            v-model="row.value"
            type="text"
            class="control mono cond-val"
            :placeholder="placeholderFor(colTypeOf(row.column))"
            @keyup.enter="canApply && onApply()"
          />
          <button class="btn btn-small btn-remove" @click="removeCondRow(i)">×</button>
        </li>
      </ul>
    </section>

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
.subtitle .ref-link {
  color: var(--accent-ink, var(--ink));
  text-decoration: none;
}
.subtitle .ref-link:hover { text-decoration: underline; }

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
  display: flex;
  align-items: baseline;
  gap: 8px;
}
.field-hint {
  font-family: var(--font-mono);
  font-size: 10.5px;
  font-weight: 400;
  letter-spacing: 0;
  text-transform: none;
  color: var(--dim);
}
.field-hint.err { color: var(--bad); }
.field-runs { flex: 0 0 240px; }
.field-runs .control.mono { width: 100%; box-sizing: border-box; }
.field-beam { flex: 0 0 220px; }
.field-beam .control.mono { width: 100%; box-sizing: border-box; }
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

.custom-card {
  margin-bottom: 18px;
  padding: 12px 14px 14px;
  background: var(--page);
  border: 1px solid var(--rule);
  border-radius: 10px;
}
.custom-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 8px;
}
.custom-title {
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 1px;
  text-transform: uppercase;
  color: var(--faint);
}
.custom-empty {
  font-size: 12px;
  color: var(--dim);
  font-style: italic;
}
.custom-empty code {
  font-family: var(--font-mono);
  font-size: 11.5px;
  background: var(--surface);
  padding: 1px 4px;
  border-radius: 3px;
  font-style: normal;
}
.cond-list { list-style: none; margin: 0; padding: 0; }
.cond-row {
  display: grid;
  grid-template-columns: minmax(180px, 1fr) 70px minmax(140px, 1.2fr) 28px;
  gap: 8px;
  margin-bottom: 6px;
  align-items: center;
}
.cond-row .control { height: 28px; padding: 0 8px; font-size: 12px; }
.cond-col { font-size: 12px; }
.cond-op { text-align: center; font-family: var(--font-mono); }
.cond-val { font-size: 12px; }
.btn-small {
  height: 28px;
  padding: 0 10px;
  font-size: 12px;
  border: 1px solid var(--rule);
  background: var(--page);
  color: var(--body);
  border-radius: 6px;
  cursor: pointer;
}
.btn-small:hover { background: var(--surface); }
.btn-remove {
  padding: 0;
  width: 28px;
  font-family: var(--font-mono);
  color: var(--faint);
}
.btn-remove:hover { color: var(--bad); }

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
