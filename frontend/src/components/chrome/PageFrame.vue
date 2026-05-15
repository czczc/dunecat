<script setup>
import AppHeader from './AppHeader.vue';
import AppSidebar from './AppSidebar.vue';
</script>

<template>
  <div class="frame">
    <AppHeader />
    <div class="body">
      <AppSidebar />
      <main class="main">
        <slot />
      </main>
    </div>
  </div>
</template>

<style scoped>
.frame {
  display: flex;
  flex-direction: column;
  height: 100vh;
}
.body {
  display: flex;
  flex: 1;
  min-height: 0;
}
.main {
  flex: 1;
  overflow-y: auto;
  background: var(--bg);
}
</style>
