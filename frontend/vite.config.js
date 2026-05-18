import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';

// Backend target. Defaults to the local app on :8000; override to
// point Vite's dev proxy at the hub:
//   DUNECAT_PROXY_TARGET=http://127.0.0.1:8001 npm run dev
const target = process.env.DUNECAT_PROXY_TARGET || 'http://127.0.0.1:8000';

export default defineConfig({
  plugins: [vue()],
  server: {
    port: 5173,
    proxy: {
      '/api': target,
      '/hub': target, // hub login / logout routes; no-op against the local app
    },
  },
});
