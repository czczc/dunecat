<script setup>
import { computed, onMounted, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { getRun } from '../api.js';

const route = useRoute();
const router = useRouter();

const runParam = computed(() => route.params.run);
const lookupInput = ref('');

const loading = ref(false);
const error = ref(null);
const data = ref(null);

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

function onLookup() {
  const trimmed = lookupInput.value.trim();
  if (!trimmed) return;
  router.push({ name: 'run-detail', params: { run: trimmed } });
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

watch(runParam, fetchRun);
onMounted(fetchRun);

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
    </div>

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
  max-width: 320px;
}
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
</style>
