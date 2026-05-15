import { createRouter, createWebHistory } from 'vue-router';
import DatasetsView from './views/DatasetsView.vue';

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', redirect: '/datasets' },
    { path: '/datasets', name: 'datasets', component: DatasetsView },
    { path: '/datasets/:detectorId', name: 'datasets-detector', component: DatasetsView },
    {
      path: '/dataset/:did/files',
      name: 'dataset-files',
      component: () => import('./views/FilesView.vue'),
    },
    {
      path: '/file/:did',
      name: 'file-detail',
      component: () => import('./views/DetailView.vue'),
    },
    {
      path: '/query',
      name: 'query',
      component: () => import('./views/QueryView.vue'),
    },
  ],
});

export default router;
