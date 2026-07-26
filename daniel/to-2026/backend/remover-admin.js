#!/usr/bin/env node
/**
 * Remove um admin e encerra as sessões dele.
 * Uso: npm run remover-admin -- <usuario>
 */
const db = require("./database");

const [, , usuarioArg] = process.argv;

if (!usuarioArg) {
  console.error("Uso: npm run remover-admin -- <usuario>");
  process.exit(1);
}

const admin = db
  .prepare("SELECT id, usuario FROM admins WHERE usuario = ?")
  .get(usuarioArg);

if (!admin) {
  console.error(`Admin "${usuarioArg}" não encontrado.`);
  process.exit(1);
}

const total = db.prepare("SELECT COUNT(*) AS n FROM admins").get().n;
if (total <= 1) {
  console.error(
    "Não é possível remover o único admin. Crie outro antes ou limpe o banco."
  );
  process.exit(1);
}

db.prepare("DELETE FROM admin_sessoes WHERE admin_id = ?").run(admin.id);
db.prepare("DELETE FROM admins WHERE id = ?").run(admin.id);

console.log(`Admin "${admin.usuario}" removido. Sessões encerradas.`);
