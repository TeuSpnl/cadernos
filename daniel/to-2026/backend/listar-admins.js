#!/usr/bin/env node
/**
 * Lista os admins cadastrados (sem exibir hash de senha).
 * Uso: npm run listar-admins
 */
const db = require("./database");

const admins = db
  .prepare(
    `SELECT id, usuario, criado_em
     FROM admins
     ORDER BY id ASC`
  )
  .all();

if (admins.length === 0) {
  console.log("Nenhum admin cadastrado.");
  process.exit(0);
}

console.log(`Admins (${admins.length}):\n`);
for (const a of admins) {
  console.log(`  #${a.id}  ${a.usuario}  (criado em ${a.criado_em})`);
}
