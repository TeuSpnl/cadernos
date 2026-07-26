const TOKEN_KEY = "cafe_admin_token";
const USER_KEY = "cafe_admin_usuario";

export function obterToken() {
  return localStorage.getItem(TOKEN_KEY) || "";
}

export function obterUsuario() {
  return localStorage.getItem(USER_KEY) || "";
}

export function salvarSessao({ token, usuario }) {
  localStorage.setItem(TOKEN_KEY, token);
  localStorage.setItem(USER_KEY, usuario);
}

export function limparSessao() {
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(USER_KEY);
}

// Fetch com Bearer token do admin
export function fetchAutenticado(url, options = {}) {
  const headers = new Headers(options.headers || {});
  const token = obterToken();
  if (token) {
    headers.set("Authorization", `Bearer ${token}`);
  }
  return fetch(url, { ...options, headers });
}
