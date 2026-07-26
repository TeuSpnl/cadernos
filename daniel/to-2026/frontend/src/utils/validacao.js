// Máscara brasileira de WhatsApp: (XX) XXXXX-XXXX ou (XX) XXXX-XXXX

export function aplicarMascaraWhatsApp(valor) {
  const digitos = String(valor || "")
    .replace(/\D/g, "")
    .slice(0, 11);

  if (digitos.length === 0) return "";
  if (digitos.length <= 2) return `(${digitos}`;
  if (digitos.length <= 6) {
    return `(${digitos.slice(0, 2)}) ${digitos.slice(2)}`;
  }
  if (digitos.length <= 10) {
    return `(${digitos.slice(0, 2)}) ${digitos.slice(2, 6)}-${digitos.slice(6)}`;
  }
  // Celular com 9 dígitos
  return `(${digitos.slice(0, 2)}) ${digitos.slice(2, 7)}-${digitos.slice(7)}`;
}

export function validarEmail(email) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(email || "").trim());
}

export function validarWhatsApp(whatsapp) {
  const digitos = String(whatsapp || "").replace(/\D/g, "");
  return digitos.length >= 10 && digitos.length <= 11;
}
