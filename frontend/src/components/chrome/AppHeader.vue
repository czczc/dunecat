<script setup>
import { RouterLink } from 'vue-router';
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
    </nav>

    <div class="search">
      <svg width="14" height="14" viewBox="0 0 14 14" aria-hidden="true">
        <circle cx="6" cy="6" r="4.5" fill="none" stroke="var(--faint)" stroke-width="1.5" />
        <line x1="9.5" y1="9.5" x2="13" y2="13" stroke="var(--faint)" stroke-width="1.5" stroke-linecap="round" />
      </svg>
      <input type="text" placeholder="Search datasets, files, runs, queries…" disabled />
    </div>

    <div class="right">
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

.search {
  flex: 1;
  max-width: 440px;
  display: flex;
  align-items: center;
  gap: 8px;
  height: 34px;
  padding: 0 10px;
  background: var(--surface);
  border: 1px solid var(--rule);
  border-radius: 8px;
}
.search input {
  flex: 1;
  border: none;
  background: transparent;
  outline: none;
  font: inherit;
  color: var(--body);
}
.search input::placeholder { color: var(--faint); }

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
