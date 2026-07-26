// Prefixo da API alinhado ao basename /to-2026
export const API_BASE = "/to-2026/api";

export function apiUrl(path) {
  const limpo = String(path || "").replace(/^\//, "");
  return `${API_BASE}/${limpo}`;
}
