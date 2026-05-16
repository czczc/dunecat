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
    <RouterLink
      :to="{ name: 'datasets' }"
      class="brand"
      aria-label="dunecat — home"
    >
      <img src="/logo/dunecat-logo.png" alt="dunecat" />
    </RouterLink>

    <nav class="nav">
      <RouterLink :to="{ name: 'datasets' }" class="nav-item" active-class="active">
        Datasets
      </RouterLink>
      <RouterLink :to="{ name: 'run-lookup' }" class="nav-item" active-class="active">
        Run
      </RouterLink>
      <RouterLink :to="{ name: 'runs-search' }" class="nav-item" active-class="active">
        Runs
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
  display: inline-flex;
  align-items: center;
  flex: 0 0 auto;
  text-decoration: none;
}
.brand img {
  height: 36px;       /* ~64% of the 56px navbar */
  width: auto;
  display: block;
}

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
