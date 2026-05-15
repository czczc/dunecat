import { createRouter, createWebHistory } from 'vue-router';
import DatasetsView from './views/DatasetsView.vue';

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', redirect: '/datasets' },
    { path: '/datasets', name: 'datasets', component: DatasetsView },
    { path: '/datasets/:detectorId', name: 'datasets-detector', component: DatasetsView },
  ],
});

export default router;
