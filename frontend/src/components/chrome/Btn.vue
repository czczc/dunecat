<script setup>
defineProps({
  variant: { type: String, default: 'default' },
  size: { type: String, default: 'md' },
});
</script>

<template>
  <button
    class="btn"
    :class="[`btn-${variant}`, `btn-${size}`]"
  >
    <slot />
  </button>
</template>

<style scoped>
.btn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  border: 1px solid var(--rule);
  background: var(--page);
  color: var(--body);
  font-family: var(--font-sans);
  font-size: 13px;
  font-weight: 500;
  white-space: nowrap;
  border-radius: 7px;
  cursor: pointer;
  transition: background 0.12s;
}
.btn:hover { background: var(--surface); }

.btn-md { height: 32px; padding: 0 14px; }
.btn-sm { height: 28px; padding: 0 10px; }

.btn-primary {
  border-color: var(--ink);
  background: var(--ink);
  color: white;
}
.btn-primary:hover { background: var(--ink); }

.btn-ghost {
  border-color: transparent;
  background: transparent;
}
.btn-ghost:hover { background: var(--surface); }
</style>
