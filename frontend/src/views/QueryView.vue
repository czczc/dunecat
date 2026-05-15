<script setup>
import { computed, onMounted, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import {
  countQuery,
  createSavedQuery,
  deleteSavedQuery,
  runQuery,
  updateSavedQuery,
  validateQuery,
} from '../api.js';
import { loadSavedQueries, nav } from '../composables/useNav.js';

const route = useRoute();
const router = useRouter();

const mql = ref('');
const queryName = ref('');
const editorRef = ref(null);

// Saved-query state
const currentSavedId = ref(null);              // null = unsaved draft
const savedName = ref('');                     // last persisted name
const savedMql = ref('');                      // last persisted mql
const saveError = ref(null);
const saving = ref(false);

const isDraft = computed(() => {
  if (currentSavedId.value == null) return true;
  return mql.value !== savedMql.value || queryName.value !== savedName.value;
});

function loadFromSaved(q) {
  currentSavedId.value = q.id;
  queryName.value = q.name;
  savedName.value = q.name;
  mql.value = q.mql;
  savedMql.value = q.mql;
  saveError.value = null;
  validateResult.value = null;
}

function resetToBlank() {
  currentSavedId.value = null;
  queryName.value = '';
  savedName.value = '';
  mql.value = '';
  savedMql.value = '';
  saveError.value = null;
  validateResult.value = null;
}

async function loadByRouteId() {
  const raw = route.query.id;
  if (!raw) {
    resetToBlank();
    return;
  }
  const id = Number(raw);
  // Try the cached list first; fall back to a fresh fetch.
  let q = nav.savedQueries.find((x) => x.id === id);
  if (!q) {
    await loadSavedQueries();
    q = nav.savedQueries.find((x) => x.id === id);
  }
  if (q) loadFromSaved(q);
  else resetToBlank();
}

watch(() => route.query.id, () => loadByRouteId());
onMounted(loadByRouteId);

// Validation
const validating = ref(false);
const validateResult = ref(null);  // null | {ok, error?}

// Run
const running = ref(false);
const runError = ref(null);
const pageData = ref(null);  // {page, page_size, rows, has_more}
const page = ref(1);
const pageSize = ref(100);
const PAGE_SIZES = [100, 200, 500];

// Count
const totalLoading = ref(false);
const totalResult = ref(null);  // null | {total, total_size}

let runToken = 0;

const SNIPPETS = [
  { label: 'Files in a dataset', mql: 'files from <namespace>:<dataset-name>' },
  { label: 'Filter by one run', mql: 'files from <namespace>:<dataset-name> where core.runs in (27731)' },
  { label: 'Filter by multiple runs', mql: 'files from <namespace>:<dataset-name> where core.runs in (27731, 27732)' },
  { label: 'Run-number range', mql: 'files from <namespace>:<dataset-name> where core.runs >= 27000 and core.runs <= 28000' },
  { label: 'Files for a run, across all datasets', mql: 'files where core.runs in (27731)' },
  { label: 'Narrowed by namespace', mql: "files where core.runs in (27731) and namespace = 'hd-protodune-det-reco'" },
  { label: 'Filter by data tier', mql: "files from <namespace>:<dataset-name> where core.data_tier = 'full-reconstructed'" },
  { label: 'Confirmed reco only', mql: "files from <namespace>:<dataset-name> where dune.output_status = 'confirmed'" },
];

function insertSnippet(text) {
  const ta = editorRef.value;
  if (!ta) {
    mql.value = (mql.value + (mql.value && !mql.value.endsWith('\n') ? '\n' : '') + text);
    return;
  }
  const start = ta.selectionStart;
  const end = ta.selectionEnd;
  const before = mql.value.slice(0, start);
  const after = mql.value.slice(end);
  mql.value = before + text + after;
  // restore caret position after the inserted text
  requestAnimationFrame(() => {
    ta.focus();
    ta.selectionStart = ta.selectionEnd = start + text.length;
  });
}

async function onValidate() {
  validating.value = true;
  validateResult.value = null;
  try {
    validateResult.value = await validateQuery(mql.value);
  } catch (e) {
    validateResult.value = { ok: false, error: e.message };
  } finally {
    validating.value = false;
  }
}

async function onRun() {
  const token = ++runToken;
  page.value = 1;
  await doFetch(token);
}

async function onSave() {
  saveError.value = null;
  saving.value = true;
  try {
    if (currentSavedId.value == null) {
      // Create a new saved query — prompt for a name if empty
      let name = queryName.value.trim();
      if (!name) {
        name = window.prompt('Save this query as:')?.trim() || '';
        if (!name) {
          saving.value = false;
          return;
        }
        queryName.value = name;
      }
      const created = await createSavedQuery(name, mql.value);
      await loadSavedQueries();
      loadFromSaved(created);
      router.replace({ name: 'query', query: { id: created.id } });
    } else {
      // Update in place
      const updated = await updateSavedQuery(currentSavedId.value, {
        name: queryName.value.trim(),
        mql: mql.value,
      });
      await loadSavedQueries();
      loadFromSaved(updated);
    }
  } catch (e) {
    saveError.value = e.message;
  } finally {
    saving.value = false;
  }
}

async function onDelete() {
  if (currentSavedId.value == null) return;
  if (!window.confirm(`Delete saved query "${savedName.value}"?`)) return;
  try {
    await deleteSavedQuery(currentSavedId.value);
    await loadSavedQueries();
    router.replace({ name: 'datasets' });
  } catch (e) {
    saveError.value = e.message;
  }
}

async function doFetch(token) {
  running.value = true;
  runError.value = null;
  try {
    const r = await runQuery(
      mql.value,
      page.value,
      pageSize.value,
      currentSavedId.value,
    );
    if (token !== runToken) return;
    pageData.value = r;
    // Reflect the new last_run_at in the sidebar without a full reload.
    if (currentSavedId.value != null) loadSavedQueries();
  } catch (e) {
    if (token !== runToken) return;
    runError.value = e.message;
    pageData.value = null;
  } finally {
    if (token === runToken) running.value = false;
  }

  // Background count
  if (page.value === 1) {
    totalLoading.value = true;
    totalResult.value = null;
    try {
      const c = await countQuery(mql.value);
      if (token !== runToken) return;
      totalResult.value = c;
    } catch {
      // silent — total stays null
    } finally {
      if (token === runToken) totalLoading.value = false;
    }
  }
}

watch(page, () => doFetch(++runToken));
watch(pageSize, () => {
  page.value = 1;
  totalResult.value = null;
  doFetch(++runToken);
});

const totalPages = computed(() => {
  if (!totalResult.value?.total || !pageData.value) return null;
  return Math.max(1, Math.ceil(totalResult.value.total / pageData.value.page_size));
});

const canNext = computed(() => {
  if (running.value) return false;
  if (totalPages.value != null) return page.value < totalPages.value;
  return pageData.value?.has_more === true;
});

function openFile(did) {
  router.push({ name: 'file-detail', params: { did } });
}

function fmtNum(n) {
  if (n == null) return '—';
  return new Intl.NumberFormat().format(n);
}
function fmtBytes(n) {
  if (n == null) return '—';
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  let i = 0; let v = n;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i += 1; }
  return `${v.toFixed(v < 10 ? 2 : 1)} ${units[i]}`;
}
function fmtTimestamp(ts) {
  if (ts == null) return '—';
  const d = typeof ts === 'number' ? new Date(ts * 1000) : new Date(ts);
  return d.toISOString().slice(0, 16).replace('T', ' ');
}
</script>

<template>
  <div class="page">
    <div class="header">
      <div class="eyebrow">Query</div>
      <div class="title-row">
        <h1 class="title">MQL query builder</h1>
        <span
          v-if="currentSavedId != null || queryName"
          class="save-status"
          :class="{ saved: !isDraft, draft: isDraft }"
        >
          {{ isDraft ? 'draft' : '✓ saved' }}
        </span>
      </div>
      <p class="subtitle">
        Run raw MQL queries against the catalog. See the cheatsheet on the right or
        click a snippet to start.
      </p>
    </div>

    <div class="grid">
      <section class="main">
        <!-- Editor -->
        <div class="card">
          <div class="card-head">
            <input
              v-model="queryName"
              class="name-input"
              placeholder="Untitled query"
            />
            <span class="head-right">
              <span
                v-if="validateResult"
                class="validate-status"
                :class="{ ok: validateResult.ok, bad: !validateResult.ok }"
              >
                <span class="dot" />
                {{ validateResult.ok ? 'linted' : 'invalid' }}
              </span>
            </span>
          </div>
          <textarea
            ref="editorRef"
            v-model="mql"
            class="editor"
            spellcheck="false"
            placeholder="files from hd-protodune-det-reco:&lt;dataset-name&gt; where core.runs in (27731)"
          />
          <div
            v-if="validateResult && !validateResult.ok"
            class="validate-error"
          >{{ validateResult.error }}</div>
          <div v-if="saveError" class="validate-error">{{ saveError }}</div>
          <div class="editor-actions">
            <button class="btn" :disabled="validating || !mql.trim()" @click="onValidate">
              {{ validating ? 'Validating…' : 'Validate' }}
            </button>
            <button
              class="btn"
              :disabled="saving || !mql.trim() || (!isDraft && currentSavedId != null)"
              @click="onSave"
            >
              {{ saving ? 'Saving…' : (currentSavedId == null ? 'Save as…' : 'Save') }}
            </button>
            <button
              v-if="currentSavedId != null"
              class="btn btn-danger"
              @click="onDelete"
            >
              Delete
            </button>
            <button
              class="btn btn-primary"
              :disabled="running || !mql.trim()"
              @click="onRun"
            >
              {{ running ? 'Running…' : '▶ Run query' }}
            </button>
          </div>
        </div>

        <!-- Results -->
        <div v-if="runError" class="error-card">
          <div class="error-title">Couldn't run query</div>
          <div class="error-detail">{{ runError }}</div>
        </div>

        <template v-else-if="pageData">
          <div class="pager">
            <div class="pager-info">
              Showing <strong>{{ (page - 1) * pageSize + 1 }}</strong>–<strong>{{
                (page - 1) * pageSize + pageData.rows.length
              }}</strong><template v-if="totalResult?.total != null">
                of <strong>{{ fmtNum(totalResult.total) }}</strong>
              </template>
            </div>
            <div class="pager-controls">
              <select v-model="pageSize" class="page-size-select">
                <option v-for="s in PAGE_SIZES" :key="s" :value="s">
                  {{ s }} / page
                </option>
              </select>
              <button class="btn" :disabled="page <= 1 || running" @click="page -= 1">
                ← Prev
              </button>
              <span class="pager-pos">
                Page {{ page }}<template v-if="totalPages != null"> / {{ totalPages }}</template>
              </span>
              <button class="btn" :disabled="!canNext" @click="page += 1">Next →</button>
            </div>
          </div>

          <div class="table-card">
            <div class="table-head">
              <div class="th col-name">File</div>
              <div class="th col-size">Size</div>
              <div class="th col-created">Created</div>
            </div>
            <div v-if="pageData.rows.length === 0" class="empty">
              No matches.
            </div>
            <div
              v-for="row in pageData.rows"
              :key="row.did"
              class="tr"
              @click="openFile(row.did)"
            >
              <div class="td col-name">
                <span class="ns">{{ row.namespace }}:</span><span class="nm">{{ row.name }}</span>
              </div>
              <div class="td col-size">{{ fmtBytes(row.size) }}</div>
              <div class="td col-created">{{ fmtTimestamp(row.created_timestamp) }}</div>
            </div>
          </div>
        </template>
      </section>

      <aside class="side">
        <!-- Snippets -->
        <section class="card">
          <div class="card-head">Snippets</div>
          <ul class="snippet-list">
            <li
              v-for="s in SNIPPETS"
              :key="s.label"
              class="snippet-row"
              @click="insertSnippet(s.mql)"
            >
              <div class="snippet-label">{{ s.label }}</div>
              <code class="snippet-mql">{{ s.mql }}</code>
            </li>
          </ul>
        </section>

        <!-- Cheatsheet -->
        <section class="card">
          <div class="card-head">MQL cheatsheet</div>
          <div class="cheat">
            <div class="cheat-row"><code>files from &lt;ns&gt;:&lt;name&gt;</code> all files in a dataset</div>
            <div class="cheat-row"><code>files where ...</code> across the whole catalog</div>
            <div class="cheat-row"><code>core.runs in (a, b)</code> list-membership / OR</div>
            <div class="cheat-row"><code>core.runs &gt;= a and core.runs &lt;= b</code> range</div>
            <div class="cheat-row"><code>key = 'string'</code> string equality (single-quoted)</div>
            <div class="cheat-row"><code>key = 42</code> numeric equality</div>
            <div class="cheat-row"><code>and · or · not</code> boolean operators</div>
            <a
              class="cheat-doc-link"
              href="https://fermitools.github.io/metacat/mql.html"
              target="_blank"
              rel="noopener noreferrer"
            >Full MQL reference ↗</a>
          </div>
        </section>

        <!-- Running total -->
        <section class="card">
          <div class="card-head">Running total</div>
          <div class="running-total">
            <div class="rt-big">
              <template v-if="totalResult?.total != null">{{ fmtNum(totalResult.total) }}</template>
              <template v-else-if="totalLoading">…</template>
              <template v-else>—</template>
            </div>
            <div class="rt-sub">
              files
              <template v-if="totalResult?.total_size">
                · {{ fmtBytes(totalResult.total_size) }}
              </template>
            </div>
          </div>
        </section>
      </aside>
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

.grid {
  display: grid;
  grid-template-columns: 1fr 320px;
  gap: 24px;
  align-items: start;
}
.main { display: flex; flex-direction: column; gap: 14px; min-width: 0; }
.side { display: flex; flex-direction: column; gap: 14px; min-width: 0; }

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

/* Editor */
.editor {
  display: block;
  width: 100%;
  height: 220px;
  padding: 12px 14px;
  border: none;
  outline: none;
  background: var(--page);
  font-family: var(--font-mono);
  font-size: 13.5px;
  color: var(--ink);
  resize: vertical;
}

.validate-status {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 11.5px;
  font-weight: 500;
  letter-spacing: 0.1px;
  text-transform: none;
}
.validate-status .dot {
  width: 6px; height: 6px; border-radius: 50%;
}
.validate-status.ok { color: var(--good); }
.validate-status.ok .dot { background: var(--good); }
.validate-status.bad { color: var(--bad); }
.validate-status.bad .dot { background: var(--bad); }

.validate-error {
  padding: 8px 14px;
  background: var(--bad-bg);
  border-top: 1px solid var(--rule);
  font-family: var(--font-mono);
  font-size: 11.5px;
  color: var(--ink);
  white-space: pre-wrap;
  word-break: break-word;
  max-height: 160px;
  overflow-y: auto;
}

.editor-actions {
  display: flex;
  gap: 8px;
  justify-content: flex-end;
  padding: 10px 14px;
  border-top: 1px solid var(--rule);
  background: var(--page);
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
.btn-primary {
  background: var(--ink);
  border-color: var(--ink);
  color: white;
}
.btn-primary:hover:not(:disabled) { background: var(--ink); }
.btn-danger {
  border-color: var(--bad);
  color: var(--bad);
}
.btn-danger:hover:not(:disabled) { background: var(--bad-bg); }

.title-row { display: flex; align-items: baseline; gap: 12px; }
.save-status {
  font-size: 11.5px;
  font-weight: 500;
  letter-spacing: 0.1px;
  padding: 2px 8px;
  border-radius: 999px;
}
.save-status.draft { background: var(--surface); color: var(--dim); }
.save-status.saved { background: var(--good-bg); color: var(--good); }

.name-input {
  border: none;
  background: transparent;
  outline: none;
  font-family: var(--font-mono);
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 1.0px;
  text-transform: uppercase;
  color: var(--faint);
  flex: 1;
  min-width: 0;
}
.name-input:focus { color: var(--ink); text-transform: none; letter-spacing: 0; }

/* Snippets */
.snippet-list { list-style: none; padding: 4px 0; margin: 0; }
.snippet-row {
  padding: 8px 14px;
  cursor: pointer;
  border-bottom: 1px solid var(--rule-soft);
  transition: background 0.12s;
}
.snippet-row:last-child { border-bottom: none; }
.snippet-row:hover { background: var(--surface); }
.snippet-label {
  font-size: 12px;
  font-weight: 500;
  color: var(--ink);
  margin-bottom: 3px;
}
.snippet-mql {
  display: block;
  font-family: var(--font-mono);
  font-size: 10.5px;
  color: var(--dim);
  white-space: pre-wrap;
  word-break: break-word;
}

/* Cheatsheet */
.cheat {
  padding: 10px 14px 12px;
  font-size: 11.5px;
  color: var(--dim);
}
.cheat-row { margin-bottom: 5px; }
.cheat code {
  font-family: var(--font-mono);
  background: var(--surface);
  padding: 1px 4px;
  border-radius: 4px;
  color: var(--ink);
  font-size: 11px;
  margin-right: 4px;
}
.cheat-doc-link {
  display: block;
  margin-top: 10px;
  font-size: 11.5px;
  color: var(--accent-ink);
  text-decoration: none;
  font-weight: 500;
}
.cheat-doc-link:hover { text-decoration: underline; }

/* Running total */
.running-total { padding: 14px; text-align: left; }
.rt-big {
  font-size: 28px;
  font-weight: 600;
  letter-spacing: -0.5px;
  color: var(--ink);
  line-height: 1;
}
.rt-sub {
  font-size: 12px;
  color: var(--dim);
  font-family: var(--font-mono);
  margin-top: 6px;
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

/* Results table */
.table-card {
  background: var(--page);
  border: 1px solid var(--rule);
  border-radius: 10px;
  overflow-x: auto;
}
.table-head, .tr {
  display: grid;
  grid-template-columns: 1fr 90px 130px;
  gap: 12px;
  padding: 8px 16px;
  align-items: center;
  min-width: 720px;
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
.col-size, .col-created {
  font-family: var(--font-mono);
  text-align: right;
  color: var(--dim);
}

.empty {
  padding: 28px;
  text-align: center;
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

.head-right { display: inline-flex; align-items: center; }
</style>
