const crypto = require("crypto");
const bcrypt = require("bcryptjs");
const db = require("./database");

// bcrypt com custo 12: hash + salt aleatório (NÃO é texto puro)
const BCRYPT_ROUNDS = 12;
const DURACAO_SESSAO_HORAS = 12;

// Hash "dummy" só para equalizar tempo quando o usuário não existe
const HASH_DUMMY =
  "$2a$12$CCCCCCCCCCCCCCCCCCCCC.6uZ5q5q5q5q5q5q5q5q5q5q5q5q5q5q5q";

function gerarTokenBruto() {
  // 256 bits de entropia criptográfica
  return crypto.randomBytes(32).toString("hex");
}

function hashToken(tokenBruto) {
  // Guardamos só o SHA-256 do token — o valor em claro nunca fica no SQLite
  return crypto.createHash("sha256").update(String(tokenBruto)).digest("hex");
}

function hashSenha(senha) {
  return bcrypt.hashSync(String(senha), BCRYPT_ROUNDS);
}

function senhaConfere(senha, senhaHash) {
  // bcrypt.compare já é resistente a timing na prática
  return bcrypt.compareSync(String(senha || ""), senhaHash);
}

function criarSessao(adminId) {
  const tokenBruto = gerarTokenBruto();
  const tokenHash = hashToken(tokenBruto);

  db.prepare(
    `INSERT INTO admin_sessoes (token, admin_id, expira_em)
     VALUES (?, ?, datetime('now', 'localtime', ?))`
  ).run(tokenHash, adminId, `+${DURACAO_SESSAO_HORAS} hours`);

  // Só o cliente recebe o token bruto
  return tokenBruto;
}

function autenticar(usuario, senha) {
  const admin = db
    .prepare("SELECT id, usuario, senha_hash FROM admins WHERE usuario = ?")
    .get(String(usuario || "").trim());

  // Sempre roda bcrypt (mesmo se usuário inexistente) para não vazar existência
  const hashParaComparar = admin ? admin.senha_hash : HASH_DUMMY;
  const ok = senhaConfere(senha, hashParaComparar);

  if (!admin || !ok) return null;

  // Garante que o que está no banco realmente é bcrypt (formato $2a$ / $2b$)
  if (!String(admin.senha_hash).startsWith("$2")) {
    console.error("ALERTA: senha_hash sem formato bcrypt — login bloqueado.");
    return null;
  }

  const token = criarSessao(admin.id);
  return { token, usuario: admin.usuario };
}

function obterAdminDoToken(tokenBruto) {
  if (!tokenBruto) return null;

  // Limpa sessões vencidas
  db.prepare(
    `DELETE FROM admin_sessoes
     WHERE datetime(expira_em) < datetime('now', 'localtime')`
  ).run();

  const tokenHash = hashToken(tokenBruto);
  const sessao = db
    .prepare(
      `SELECT s.token, a.id, a.usuario
       FROM admin_sessoes s
       JOIN admins a ON a.id = s.admin_id
       WHERE s.token = ?
         AND datetime(s.expira_em) >= datetime('now', 'localtime')`
    )
    .get(tokenHash);

  return sessao || null;
}

function encerrarSessao(tokenBruto) {
  if (!tokenBruto) return;
  db.prepare("DELETE FROM admin_sessoes WHERE token = ?").run(
    hashToken(tokenBruto)
  );
}

function extrairToken(req) {
  const header = req.headers.authorization || "";
  return header.startsWith("Bearer ") ? header.slice(7).trim() : "";
}

// Anexa admin ao request se o token for válido; não responde sozinho
function anexarAdmin(req) {
  const token = extrairToken(req);
  const admin = obterAdminDoToken(token);
  if (!admin) {
    req.admin = null;
    req.token = null;
    return false;
  }
  req.admin = { id: admin.id, usuario: admin.usuario };
  req.token = token;
  return true;
}

// Middleware clássico (sem log)
function exigirAuth(req, res, next) {
  if (!anexarAdmin(req)) {
    return res.status(401).json({
      sucesso: false,
      erros: ["Não autorizado. Faça login no painel."],
    });
  }
  return next();
}

module.exports = {
  autenticar,
  exigirAuth,
  anexarAdmin,
  encerrarSessao,
  obterAdminDoToken,
  hashSenha,
  extrairToken,
  BCRYPT_ROUNDS,
};
