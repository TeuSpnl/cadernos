import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// App sob /to-2026/ — proxy encaminha a API para o Express
export default defineConfig({
  base: "/to-2026/",
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/to-2026/api": {
        target: "http://localhost:3001",
        changeOrigin: true,
      },
    },
  },
});
