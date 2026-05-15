<script setup>
import { computed, onMounted, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { getFile } from '../api.js';
import { loadDetectors, nav } from '../composables/useNav.js';

const route = useRoute();
const router = useRouter();

const did = computed(() => route.params.did);
const namespace = computed(() => did.value?.split(':')[0] || '');
const name = computed(() => did.value?.split(':').slice(1).join(':') || '');

const detector = computed(() =>
  nav.detectors.find((d) => d.namespaces.includes(namespace.value)) || null,
);

const loading = ref(false);
const error = ref(null);
const file = ref(null);
const copyState = ref('idle');  // 'idle' | 'copied'

const sortedMetadata = computed(() => {
  if (!file.value?.metadata) return [];
  return Object.entries(file.value.metadata).sort(([a], [b]) =>
    a.localeCompare(b),
  );
});

const rucioCommand = computed(() =>
  file.value ? `rucio download dune:${file.value.name}` : '',
);

async function fetchFile() {
  if (!did.value) return;
  loading.value = true;
  error.value = null;
  try {
    file.value = await getFile(did.value);
  } catch (e) {
    error.value = e;
    file.value = null;
  } finally {
    loading.value = false;
  }
}

watch(did, fetchFile);

onMounted(async () => {
  await loadDetectors();
  fetchFile();
});

function gotoDetector() {
  if (detector.value) {
    router.push({
      name: 'datasets-detector',
      params: { detectorId: detector.value.id },
    });
  }
}

function gotoDataset(datasetDid) {
  router.push({ name: 'dataset-files', params: { did: datasetDid } });
}

async function copyRucio() {
  try {
    await navigator.clipboard.writeText(rucioCommand.value);
    copyState.value = 'copied';
    setTimeout(() => (copyState.value = 'idle'), 1500);
  } catch {
    copyState.value = 'idle';
  }
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
  return d.toISOString().replace('T', ' ').slice(0, 19) + ' UTC';
}
function fmtValue(v) {
  if (v == null) return '—';
  if (Array.isArray(v)) return v.join(', ');
  if (typeof v === 'object') return JSON.stringify(v);
  return String(v);
}
function datasetDid(d) {
  return `${d.namespace}:${d.name}`;
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
        <span class="crumb">{{ namespace }}</span>
        <span class="sep">›</span>
        <span class="crumb">file</span>
      </div>
      <h1 class="title">{{ name || did }}</h1>
      <p class="subtitle" v-if="file">
        {{ fmtBytes(file.size) }} · created {{ fmtTimestamp(file.created_timestamp) }}
      </p>
      <div class="actions">
        <button class="btn" @click="router.back()">← Back</button>
      </div>
    </div>

    <div v-if="error?.status === 404" class="placeholder">
      <div class="placeholder-title">File not found</div>
      <div class="placeholder-body">{{ did }}</div>
    </div>

    <div v-else-if="error" class="error-card">
      <div class="error-title">Couldn't load file</div>
      <div class="error-detail">{{ error.message }}</div>
    </div>

    <div v-else-if="loading" class="placeholder">
      <div class="placeholder-body">Loading…</div>
    </div>

    <div v-else-if="file" class="grid">
      <!-- Left: metadata -->
      <section class="card">
        <div class="card-head">
          Metadata · {{ sortedMetadata.length }} fields
        </div>
        <div class="metadata-grid">
          <template v-for="[k, v] in sortedMetadata" :key="k">
            <div class="meta-key">{{ k }}</div>
            <div class="meta-val">{{ fmtValue(v) }}</div>
          </template>
        </div>
        <div v-if="sortedMetadata.length === 0" class="card-empty">
          No metadata on this file.
        </div>
      </section>

      <!-- Right: stack -->
      <aside class="side">
        <section class="card">
          <div class="card-head">
            Parent datasets · {{ (file.datasets || []).length }}
          </div>
          <ul class="ds-list">
            <li
              v-for="d in file.datasets || []"
              :key="datasetDid(d)"
              class="ds-row"
              @click="gotoDataset(datasetDid(d))"
            >
              <span class="ds-ns">{{ d.namespace }}:</span><span class="ds-name">{{ d.name }}</span>
            </li>
          </ul>
          <div v-if="(file.datasets || []).length === 0" class="card-empty">
            No parent datasets.
          </div>
        </section>

        <section class="card">
          <div class="card-head">File info</div>
          <div class="kv-grid">
            <div class="kv-key">fid</div>
            <div class="kv-val mono">{{ file.fid || '—' }}</div>
            <div class="kv-key">size</div>
            <div class="kv-val mono">{{ fmtBytes(file.size) }}</div>
            <div class="kv-key">created</div>
            <div class="kv-val mono">{{ fmtTimestamp(file.created_timestamp) }}</div>
            <div class="kv-key">updated</div>
            <div class="kv-val mono">{{ fmtTimestamp(file.updated_timestamp) }}</div>
            <div class="kv-key">retired</div>
            <div class="kv-val mono">{{ file.retired ? 'yes' : 'no' }}</div>
            <template v-for="(checksum, algo) in file.checksums || {}" :key="algo">
              <div class="kv-key">{{ algo }}</div>
              <div class="kv-val mono">{{ checksum }}</div>
            </template>
          </div>
        </section>

        <section class="card">
          <div class="card-head">Download</div>
          <p class="card-body-text">
            Copy this command into your terminal (Rucio must be configured locally).
          </p>
          <div class="rucio-row">
            <code class="rucio-cmd">{{ rucioCommand }}</code>
            <button class="btn btn-copy" @click="copyRucio">
              {{ copyState === 'copied' ? '✓ Copied' : 'Copy' }}
            </button>
          </div>
        </section>

        <section class="card card-deferred">
          <div class="card-head">Lineage · Replicas · Schema peek</div>
          <p class="card-body-text">
            Deferred — these need data sources beyond metacat (Rucio for
            replicas, file content inspection for schema peeks).
          </p>
        </section>
      </aside>
    </div>
  </div>
</template>

<style scoped>
.page { padding: 22px 36px 28px; }

/* Header */
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
  padding-right: 100px;
}
.subtitle { font-size: 13px; color: var(--dim); margin: 0; }
.actions { position: absolute; top: 0; right: 0; }

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
.btn:hover { background: var(--surface); }

.btn-copy { min-width: 80px; justify-content: center; }

/* Grid */
.grid {
  display: grid;
  grid-template-columns: 1.25fr 1fr;
  gap: 24px;
  align-items: start;
}
.side { display: flex; flex-direction: column; gap: 14px; min-width: 0; }

/* Card */
.card {
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
.card-empty {
  padding: 16px 14px;
  font-size: 12.5px;
  color: var(--faint);
  font-style: italic;
}
.card-body-text {
  padding: 10px 14px 4px;
  font-size: 12px;
  color: var(--dim);
  margin: 0;
}

.card-deferred .card-body-text {
  padding-bottom: 12px;
  font-style: italic;
}

/* Metadata grid — auto-flowing 2-column */
.metadata-grid {
  padding: 12px 14px;
  column-count: 2;
  column-gap: 28px;
}
.meta-key, .meta-val {
  display: inline-block;
  width: 100%;
  break-inside: avoid;
}
.meta-key {
  font-size: 11px;
  color: var(--dim);
  margin-top: 4px;
}
.meta-val {
  font-family: var(--font-mono);
  font-size: 11.5px;
  color: var(--ink);
  margin-bottom: 6px;
  word-break: break-word;
}

/* Datasets list */
.ds-list { list-style: none; margin: 0; padding: 4px 0; }
.ds-row {
  padding: 6px 14px;
  font-size: 12px;
  cursor: pointer;
  transition: background 0.12s;
  word-break: break-all;
  line-height: 1.4;
}
.ds-row:hover { background: var(--surface); }
.ds-ns { font-family: var(--font-mono); color: var(--faint); font-size: 10.5px; }
.ds-name { font-family: var(--font-mono); color: var(--ink); }

/* Key-value grid */
.kv-grid {
  display: grid;
  grid-template-columns: 80px 1fr;
  gap: 4px 12px;
  padding: 12px 14px;
}
.kv-key { font-size: 11px; color: var(--dim); }
.kv-val { font-size: 12px; color: var(--ink); word-break: break-all; }
.mono { font-family: var(--font-mono); font-size: 11.5px; }

/* Rucio command */
.rucio-row {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 14px 12px;
}
.rucio-cmd {
  flex: 1;
  display: block;
  background: var(--surface);
  border: 1px solid var(--rule);
  border-radius: 6px;
  padding: 6px 10px;
  font-family: var(--font-mono);
  font-size: 11.5px;
  color: var(--ink);
  word-break: break-all;
  user-select: all;
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
.placeholder-body { font-size: 13.5px; color: var(--dim); font-family: var(--font-mono); word-break: break-all; }

.error-card {
  padding: 14px 16px;
  background: var(--bad-bg);
  border: 1px solid var(--bad);
  border-radius: 10px;
}
.error-title { font-weight: 600; color: var(--bad); margin-bottom: 4px; }
.error-detail { font-family: var(--font-mono); font-size: 12px; color: var(--ink); }
</style>
