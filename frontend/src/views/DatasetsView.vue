<script setup>
import { computed } from 'vue';
import { useRoute } from 'vue-router';

const route = useRoute();
const detectorId = computed(() => route.params.detectorId);
</script>

<template>
  <div class="page">
    <div class="header">
      <div class="eyebrow">Datasets</div>
      <h1 class="title">Browse datasets</h1>
      <p class="subtitle">
        Pick a sub-detector, then open a dataset to see its files.
      </p>
    </div>

    <div class="placeholder">
      <div class="placeholder-title">
        {{ detectorId ? `Datasets for ${detectorId}` : 'No detector selected' }}
      </div>
      <div class="placeholder-body">
        Datasets view coming in F2 — pick a detector from the sidebar to see its
        datasets here. The list will paginate against the cached
        <code>list_datasets()</code> path with a "Refresh" button.
      </div>
    </div>
  </div>
</template>

<style scoped>
.page {
  padding: 22px 36px 28px;
}
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
.subtitle {
  font-size: 13.5px;
  color: var(--dim);
  margin: 0;
}

.placeholder {
  padding: 28px;
  background: var(--page);
  border: 1px solid var(--rule);
  border-radius: 10px;
}
.placeholder-title {
  font-size: 14px;
  font-weight: 600;
  color: var(--ink);
  margin-bottom: 6px;
}
.placeholder-body {
  font-size: 13.5px;
  color: var(--dim);
  max-width: 540px;
}
.placeholder-body code {
  font-family: var(--font-mono);
  font-size: 12px;
  background: var(--surface);
  padding: 1px 5px;
  border-radius: 4px;
}
</style>
