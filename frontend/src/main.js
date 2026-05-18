import { createApp } from 'vue';
import App from './App.vue';
import router from './router.js';
import { fetchConfig, setConfig } from './api.js';
import './styles/tokens.css';
import './styles/base.css';

// Bootstrap: learn whether we're talking to the local app or the hub
// before any view fetches kick off. `fetchConfig` tolerates network
// errors by returning {mode: 'local'} so the SPA still renders when
// the backend is briefly unreachable.
fetchConfig().then((cfg) => {
  setConfig(cfg);
  createApp(App).use(router).mount('#app');
});
