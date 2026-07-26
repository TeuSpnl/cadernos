#!/usr/bin/env node
/**
 * Cria (ou atualiza) um usuário admin pelo terminal.
 *
 * Preferido (senha NÃO vai para o histórico do shell):
 *   npm run criar-admin -- <usuario>
 *   (depois digita a senha ocultamente)
 *
 * Alternativa (menos segura — senha fica no histórico):
 *   npm run criar-admin -- <usuario> <senha>
 */
const readline = require("readline");
const { hashSenha, BCRYPT_ROUNDS } = require("./auth");
const db = require("./database");

const [, , usuarioArg, senhaArg] = process.argv;

function lerSenhaOculta(pergunta) {
  return new Promise((resolve) => {
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
    });

    // stdin em modo raw para não ecoar a senha
    const stdin = process.stdin;
    const estavaRaw = stdin.isRaw;
    if (stdin.isTTY) stdin.setRawMode(true);

    process.stdout.write(pergunta);
    let senha = "";

    const onData = (char) => {
      const c = char.toString("utf8");

      if (c === "\n" || c === "\r" || c === "\u0004") {
        stdin.removeListener("data", onData);
        if (stdin.isTTY) stdin.setRawMode(Boolean(estavaRaw));
        rl.close();
        process.stdout.write("\n");
        resolve(senha);
        return;
      }

      // Ctrl+C
      if (c === "\u0003") {
        process.stdout.write("\n");
        process.exit(1);
      }

      // Backspace
      if (c === "\u007f" || c === "\b") {
        senha = senha.slice(0, -1);
        return;
      }

      senha += c;
    };

    stdin.on("data", onData);
  });
}

async function main() {
  if (!usuarioArg) {
    console.error("Uso: npm run criar-admin -- <usuario>");
    console.error("     (a senha será pedida de forma oculta)");
    process.exit(1);
  }

  let senha = senhaArg;

  if (senha) {
    console.warn(
      "Aviso: senha passada na linha de comando pode ficar no histórico do shell."
    );
    console.warn("Prefira: npm run criar-admin -- <usuario>");
  } else {
    if (!process.stdin.isTTY) {
      console.error("Sem TTY: informe a senha como 2º argumento ou use um terminal interativo.");
      process.exit(1);
    }
    senha = await lerSenhaOculta("Senha (não será exibida): ");
    const confirmacao = await lerSenhaOculta("Confirme a senha: ");
    if (senha !== confirmacao) {
      console.error("As senhas não coincidem.");
      process.exit(1);
    }
  }

  if (String(senha).length < 8) {
    console.error("A senha precisa ter pelo menos 8 caracteres.");
    process.exit(1);
  }

  // Só o hash bcrypt (com salt) vai para o banco — nunca a senha em claro
  const senhaHash = hashSenha(senha);
  const existente = db
    .prepare("SELECT id FROM admins WHERE usuario = ?")
    .get(usuarioArg);

  if (existente) {
    // Invalida sessões antigas ao trocar a senha
    db.prepare("DELETE FROM admin_sessoes WHERE admin_id = ?").run(existente.id);
    db.prepare("UPDATE admins SET senha_hash = ? WHERE id = ?").run(
      senhaHash,
      existente.id
    );
    console.log(
      `Senha do admin "${usuarioArg}" atualizada (bcrypt, custo ${BCRYPT_ROUNDS}). Sessões anteriores encerradas.`
    );
  } else {
    db.prepare("INSERT INTO admins (usuario, senha_hash) VALUES (?, ?)").run(
      usuarioArg,
      senhaHash
    );
    console.log(
      `Admin "${usuarioArg}" criado (senha armazenada com bcrypt, custo ${BCRYPT_ROUNDS}).`
    );
  }

  // Sanidade: confirma que não salvamos texto puro
  if (!senhaHash.startsWith("$2")) {
    console.error("Falha interna: hash gerado sem formato bcrypt.");
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
