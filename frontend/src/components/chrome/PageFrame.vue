<script setup>
import AppHeader from './AppHeader.vue';
</script>

<template>
  <div class="frame">
    <AppHeader />
    <main class="main">
      <slot />
    </main>
  </div>
</template>

<style scoped>
.frame {
  display: flex;
  flex-direction: column;
  height: 100vh;
}
.main {
  flex: 1;
  overflow-y: auto;
  background: var(--bg);
  min-height: 0;
}
</style>
