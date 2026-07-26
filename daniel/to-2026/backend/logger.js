const fs = require("fs");
const path = require("path");
const db = require("./database");

const LOG_DIR = path.join(__dirname, "logs");
const LOG_FILE = path.join(LOG_DIR, "admin.log");

// Garante pasta de logs
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}

function extrairIp(req) {
  const forwarded = req.headers["x-forwarded-for"];
  if (typeof forwarded === "string" && forwarded.length > 0) {
    return forwarded.split(",")[0].trim();
  }
  return req.socket?.remoteAddress || req.ip || "";
}

/**
 * Registra ação administrativa no SQLite e em logs/admin.log.
 * Nunca recebe nem grava senha.
 */
function registrarAdminLog(req, { acao, usuario = null, detalhes = null }) {
  const ip = req ? extrairIp(req) : "";
  const userAgent = req?.headers?.["user-agent"] || "";
  const usuarioFinal =
    usuario || req?.admin?.usuario || null;
  const detalhesStr =
    detalhes == null
      ? null
      : typeof detalhes === "string"
        ? detalhes
        : JSON.stringify(detalhes);

  try {
    db.prepare(
      `INSERT INTO admin_logs (acao, usuario, detalhes, ip, user_agent)
       VALUES (?, ?, ?, ?, ?)`
    ).run(acao, usuarioFinal, detalhesStr, ip, userAgent);
  } catch (erro) {
    console.error("Falha ao gravar admin_logs:", erro.message);
  }

  // Linha legível para tail -f logs/admin.log
  const linha = [
    new Date().toISOString(),
    acao,
    usuarioFinal || "-",
    ip || "-",
    detalhesStr || "-",
  ].join(" | ");

  try {
    fs.appendFileSync(LOG_FILE, linha + "\n", "utf8");
  } catch (erro) {
    console.error("Falha ao gravar admin.log:", erro.message);
  }
}

module.exports = { registrarAdminLog, LOG_FILE };
