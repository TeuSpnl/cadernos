const Database = require("better-sqlite3");
const path = require("path");

// Banco SQLite local — arquivo na mesma pasta do backend
const dbPath = path.join(__dirname, "database.sqlite");
const db = new Database(dbPath);

// Tabela de inscritos do evento
db.exec(`
  CREATE TABLE IF NOT EXISTS inscricoes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL,
    nome TEXT NOT NULL,
    empresa TEXT NOT NULL,
    whatsapp TEXT NOT NULL,
    data_inscricao TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
  )
`);

// Usuários admin — senha_hash = bcrypt (nunca texto puro)
db.exec(`
  CREATE TABLE IF NOT EXISTS admins (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    usuario TEXT NOT NULL UNIQUE,
    senha_hash TEXT NOT NULL,
    criado_em TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
  )
`);

// Sessões: coluna "token" guarda SHA-256 do token bruto (não o valor em claro)
db.exec(`
  CREATE TABLE IF NOT EXISTS admin_sessoes (
    token TEXT PRIMARY KEY,
    admin_id INTEGER NOT NULL,
    criado_em TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
    expira_em TEXT NOT NULL,
    FOREIGN KEY (admin_id) REFERENCES admins(id)
  )
`);

// Auditoria de ações do admin (somente log — sem tela)
db.exec(`
  CREATE TABLE IF NOT EXISTS admin_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    acao TEXT NOT NULL,
    usuario TEXT,
    detalhes TEXT,
    ip TEXT,
    user_agent TEXT,
    criado_em TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
  )
`);

module.exports = db;
