const express = require("express");
const ExcelJS = require("exceljs");
const db = require("./database");
const { autenticar, anexarAdmin, encerrarSessao } = require("./auth");
const { registrarAdminLog } = require("./logger");

const rotas = express.Router();

// Validação básica dos campos obrigatórios
function validarInscricao({ email, nome, empresa, whatsapp }) {
  const erros = [];

  if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email.trim())) {
    erros.push("E-mail inválido.");
  }
  if (!nome || nome.trim().length < 2) {
    erros.push("Nome é obrigatório.");
  }
  if (!empresa || empresa.trim().length < 1) {
    erros.push("Empresa é obrigatória.");
  }
  // Aceita WhatsApp com ou sem máscara (só dígitos contam)
  const digitos = String(whatsapp || "").replace(/\D/g, "");
  if (digitos.length < 10 || digitos.length > 11) {
    erros.push("WhatsApp inválido.");
  }

  return erros;
}

// Protege rota, registra acesso negado e segue para o handler
function comAuth(handler) {
  return (req, res, next) => {
    if (!anexarAdmin(req)) {
      registrarAdminLog(req, {
        acao: "acesso_negado",
        detalhes: { rota: req.originalUrl, metodo: req.method },
      });
      return res.status(401).json({
        sucesso: false,
        erros: ["Não autorizado. Faça login no painel."],
      });
    }
    return handler(req, res, next);
  };
}

// Login do painel admin — resposta só com token; senha nunca é persistida nem logada
rotas.post("/admin/login", (req, res) => {
  const { usuario, senha } = req.body || {};

  if (!usuario || !senha) {
    registrarAdminLog(req, {
      acao: "login_falha",
      usuario: usuario || null,
      detalhes: { motivo: "campos_ausentes" },
    });
    return res.status(400).json({
      sucesso: false,
      erros: ["Informe usuário e senha."],
    });
  }

  const sessao = autenticar(usuario, senha);
  if (!sessao) {
    registrarAdminLog(req, {
      acao: "login_falha",
      usuario: String(usuario).trim(),
      detalhes: { motivo: "credenciais_invalidas" },
    });
    return res.status(401).json({
      sucesso: false,
      erros: ["Usuário ou senha inválidos."],
    });
  }

  registrarAdminLog(req, {
    acao: "login_sucesso",
    usuario: sessao.usuario,
  });

  return res.json({
    sucesso: true,
    token: sessao.token,
    usuario: sessao.usuario,
  });
});

// Logout (invalida o token atual)
rotas.post(
  "/admin/logout",
  comAuth((req, res) => {
    registrarAdminLog(req, { acao: "logout" });
    encerrarSessao(req.token);
    return res.json({ sucesso: true });
  })
);

// Confirma se o token ainda vale (acesso ao painel)
rotas.get(
  "/admin/me",
  comAuth((req, res) => {
    registrarAdminLog(req, { acao: "acesso_painel" });
    return res.json({ sucesso: true, usuario: req.admin.usuario });
  })
);

// Cria uma nova inscrição (público)
rotas.post("/inscricoes", (req, res) => {
  const { email, nome, empresa, whatsapp } = req.body || {};
  const erros = validarInscricao({ email, nome, empresa, whatsapp });

  if (erros.length > 0) {
    return res.status(400).json({ sucesso: false, erros });
  }

  try {
    const stmt = db.prepare(`
      INSERT INTO inscricoes (email, nome, empresa, whatsapp)
      VALUES (?, ?, ?, ?)
    `);
    const result = stmt.run(
      email.trim().toLowerCase(),
      nome.trim(),
      empresa.trim(),
      whatsapp.trim()
    );

    return res.status(201).json({
      sucesso: true,
      id: result.lastInsertRowid,
      mensagem: "Inscrição realizada com sucesso.",
    });
  } catch (erro) {
    console.error("Erro ao salvar inscrição:", erro);
    return res.status(500).json({
      sucesso: false,
      erros: ["Erro interno ao salvar a inscrição."],
    });
  }
});

// Lista todas as inscrições (protegido)
rotas.get(
  "/inscricoes",
  comAuth((req, res) => {
    try {
      const inscritos = db
        .prepare(
          `SELECT id, email, nome, empresa, whatsapp, data_inscricao
           FROM inscricoes
           ORDER BY datetime(data_inscricao) DESC`
        )
        .all();

      registrarAdminLog(req, {
        acao: "listar_inscritos",
        detalhes: { total: inscritos.length },
      });

      return res.json({ sucesso: true, dados: inscritos });
    } catch (erro) {
      console.error("Erro ao listar inscrições:", erro);
      return res.status(500).json({
        sucesso: false,
        erros: ["Erro interno ao listar as inscrições."],
      });
    }
  })
);

// Exporta inscritos para Excel (.xlsx) — protegido
rotas.get(
  "/inscricoes/export",
  comAuth(async (req, res) => {
    try {
      const inscritos = db
        .prepare(
          `SELECT id, email, nome, empresa, whatsapp, data_inscricao
           FROM inscricoes
           ORDER BY datetime(data_inscricao) DESC`
        )
        .all();

      const workbook = new ExcelJS.Workbook();
      workbook.creator = "Café com Empresários";
      const sheet = workbook.addWorksheet("Inscritos");

      sheet.columns = [
        { header: "ID", key: "id", width: 8 },
        { header: "Email", key: "email", width: 32 },
        { header: "Nome", key: "nome", width: 28 },
        { header: "Empresa", key: "empresa", width: 28 },
        { header: "WhatsApp", key: "whatsapp", width: 18 },
        { header: "Data da Inscrição", key: "data_inscricao", width: 22 },
      ];

      // Cabeçalho em destaque
      sheet.getRow(1).font = { bold: true };

      for (const item of inscritos) {
        sheet.addRow(item);
      }

      const buffer = await workbook.xlsx.writeBuffer();

      registrarAdminLog(req, {
        acao: "exportar_excel",
        detalhes: { total: inscritos.length },
      });

      res.setHeader(
        "Content-Disposition",
        'attachment; filename="inscritos-cafe-empresarios.xlsx"'
      );
      res.setHeader(
        "Content-Type",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
      );
      return res.send(Buffer.from(buffer));
    } catch (erro) {
      console.error("Erro ao exportar Excel:", erro);
      return res.status(500).json({
        sucesso: false,
        erros: ["Erro interno ao exportar o Excel."],
      });
    }
  })
);

module.exports = rotas;
