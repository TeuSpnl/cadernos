const db = require("./database");
const { INSCRICOES_ABERTAS: PADRAO_CONFIG } = require("./config");

const CHAVE = "inscricoes_abertas";

// Garante valor inicial no banco (só na primeira vez)
function garantirPadrao() {
  const row = db.prepare("SELECT valor FROM configuracoes WHERE chave = ?").get(CHAVE);
  if (!row) {
    db.prepare(
      `INSERT INTO configuracoes (chave, valor) VALUES (?, ?)`
    ).run(CHAVE, PADRAO_CONFIG ? "1" : "0");
  }
}

garantirPadrao();

function inscricoesEstaoAbertas() {
  const row = db.prepare("SELECT valor FROM configuracoes WHERE chave = ?").get(CHAVE);
  return row ? row.valor === "1" : Boolean(PADRAO_CONFIG);
}

function definirInscricoesAbertas(aberto) {
  const valor = aberto ? "1" : "0";
  db.prepare(
    `INSERT INTO configuracoes (chave, valor, atualizado_em)
     VALUES (?, ?, datetime('now', 'localtime'))
     ON CONFLICT(chave) DO UPDATE SET
       valor = excluded.valor,
       atualizado_em = excluded.atualizado_em`
  ).run(CHAVE, valor);
  return inscricoesEstaoAbertas();
}

module.exports = {
  inscricoesEstaoAbertas,
  definirInscricoesAbertas,
};
