<script setup>
import { onMounted } from 'vue';
import { RouterLink, useRouter } from 'vue-router';
import { nav, loadSavedQueries } from '../../composables/useNav.js';

const router = useRouter();

onMounted(() => {
  loadSavedQueries();
});

function onSavedPicked(e) {
  const raw = e.target.value;
  e.target.value = '';
  if (!raw) return;
  if (raw === '__new__') {
    router.push({ name: 'query' });
  } else {
    router.push({ name: 'query', query: { id: Number(raw) } });
  }
}
</script>

<template>
  <header class="header">
    <RouterLink :to="{ name: 'datasets' }" class="brand">
      <svg width="22" height="22" viewBox="0 0 22 22" aria-hidden="true">
        <rect x="2" y="4" width="18" height="3" rx="1" fill="none" stroke="var(--ink)" stroke-width="1.5" />
        <rect x="2" y="9.5" width="18" height="3" rx="1" fill="var(--ink)" fill-opacity="0.08" stroke="var(--ink)" stroke-width="1.5" />
        <rect x="2" y="15" width="18" height="3" rx="1" fill="none" stroke="var(--ink)" stroke-width="1.5" />
      </svg>
      <span class="brand-name">dunecat</span>
      <span class="brand-sub">· DUNE file catalog</span>
    </RouterLink>

    <nav class="nav">
      <RouterLink :to="{ name: 'datasets' }" class="nav-item" active-class="active">
        Datasets
      </RouterLink>
      <RouterLink :to="{ name: 'run-lookup' }" class="nav-item" active-class="active">
        Run
      </RouterLink>
      <RouterLink :to="{ name: 'query' }" class="nav-item" active-class="active">
        Query
      </RouterLink>
    </nav>

    <div class="right">
      <select class="saved-picker" :value="''" @change="onSavedPicked">
        <option value="" disabled>
          {{ nav.savedQueries.length ? `★ Saved queries (${nav.savedQueries.length})…` : '★ Saved queries' }}
        </option>
        <option value="__new__">+ New query</option>
        <option v-for="q in nav.savedQueries" :key="q.id" :value="q.id">
          ★ {{ q.name }}
        </option>
      </select>
      <div class="user">CZ</div>
    </div>
  </header>
</template>

<style scoped>
.header {
  display: flex;
  align-items: center;
  gap: 22px;
  height: 56px;
  padding: 0 20px;
  background: var(--page);
  border-bottom: 1px solid var(--rule);
}

.brand {
  display: flex;
  align-items: center;
  gap: 8px;
  text-decoration: none;
  color: var(--ink);
  flex-shrink: 0;
  white-space: nowrap;
}
.brand-name { font-size: 16px; font-weight: 600; }
.brand-sub { font-size: 12px; color: var(--faint); }

.nav { display: flex; gap: 22px; flex-shrink: 0; }
.nav-item {
  position: relative;
  color: var(--dim);
  text-decoration: none;
  font-size: 13.5px;
  font-weight: 500;
  padding: 4px 0;
}
.nav-item.active {
  color: var(--ink);
  font-weight: 600;
}
.nav-item.active::after {
  content: '';
  position: absolute;
  left: 0; right: 0;
  bottom: -1px;
  height: 2px;
  background: var(--accent);
}

.saved-picker {
  height: 30px;
  padding: 0 10px;
  border: 1px solid var(--rule);
  background: var(--page);
  border-radius: 8px;
  font-family: var(--font-sans);
  font-size: 12.5px;
  color: var(--body);
  max-width: 260px;
  cursor: pointer;
}
.saved-picker:hover { background: var(--surface); }

.right {
  display: flex;
  align-items: center;
  gap: 14px;
  flex-shrink: 0;
  margin-left: auto;
}
.user {
  width: 28px; height: 28px;
  border-radius: 50%;
  background: var(--accent);
  color: white;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 11.5px;
  font-weight: 600;
}
</style>
