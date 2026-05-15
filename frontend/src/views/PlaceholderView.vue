<script setup>
import { useRoute } from 'vue-router';
const route = useRoute();
</script>

<template>
  <div class="page">
    <div class="header">
      <div class="eyebrow">Files</div>
      <h1 class="title">{{ route.params.did }}</h1>
    </div>
    <div class="placeholder">
      <div class="placeholder-body">
        File detail view lands in F4 — this route is a placeholder for clicks coming from FilesView.
      </div>
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
  font-family: var(--font-mono);
  font-size: 18px;
  font-weight: 500;
  margin: 0;
  color: var(--ink);
  word-break: break-all;
}
.placeholder {
  padding: 28px;
  background: var(--page);
  border: 1px solid var(--rule);
  border-radius: 10px;
  text-align: center;
}
.placeholder-body { font-size: 13.5px; color: var(--dim); }
</style>
