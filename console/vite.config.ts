import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Requests go through this dev proxy rather than straight to :8081, so the browser sees one
// origin and no CORS configuration is needed on the services. That matters beyond convenience:
// the alternative is relaxing a security config to accommodate a development tool, which is how
// permissive CORS ends up shipped. The proxy exists only in `vite dev`.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': {
        target: 'http://localhost:8081',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ''),
      },
    },
  },
})
