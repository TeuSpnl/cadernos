const express = require("express");
const ExcelJS = require("exceljs");
const db = require("./database");
const { autenticar, anexarAdmin, encerrarSessao } = require("./auth");
const { registrarAdminLog } = require("./logger");
const { inscricoesEstaoAbertas, definirInscricoesAbertas } = require("./settings");

const rotas = express.Router();

// Validação básica dos campos obrigatórios
function validarCadastro({ email, nome, empresa, whatsapp }) {
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

async function exportarPlanilha({ linhas, aba, colunas, arquivo, res }) {
  const workbook = new ExcelJS.Workbook();
  workbook.creator = "Café com Empresários";
  const sheet = workbook.addWorksheet(aba);
  sheet.columns = colunas;
  sheet.getRow(1).font = { bold: true };
  for (const item of linhas) {
    sheet.addRow(item);
  }
  const buffer = await workbook.xlsx.writeBuffer();
  res.setHeader("Content-Disposition", `attachment; filename="${arquivo}"`);
  res.setHeader(
    "Content-Type",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
  );
  return res.send(Buffer.from(buffer));
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

// Status público: front usa para escolher o formulário (sem rebuild)
rotas.get("/status", (_req, res) => {
  return res.json({
    sucesso: true,
    inscricoesAbertas: inscricoesEstaoAbertas(),
  });
});

// Admin: liga/desliga inscrição do evento (persiste no SQLite)
rotas.put(
  "/admin/inscricoes-status",
  comAuth((req, res) => {
    const aberto = Boolean(req.body?.abertas);
    const atual = definirInscricoesAbertas(aberto);

    registrarAdminLog(req, {
      acao: atual ? "abrir_inscricoes" : "fechar_inscricoes",
      detalhes: { inscricoesAbertas: atual },
    });

    return res.json({
      sucesso: true,
      inscricoesAbertas: atual,
      mensagem: atual
        ? "Inscrições do evento reabertas."
        : "Inscrições do evento encerradas. O site mostra o formulário de interesse.",
    });
  })
);

// Inscrições do evento — só aceita se o status estiver aberto
rotas.post("/inscricoes", (req, res) => {
  if (!inscricoesEstaoAbertas()) {
    return res.status(403).json({
      sucesso: false,
      erros: [
        "As inscrições para esta edição estão encerradas. Use o formulário de interesse.",
      ],
    });
  }

  const { email, nome, empresa, whatsapp } = req.body || {};
  const erros = validarCadastro({ email, nome, empresa, whatsapp });

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

// Lista inscritos do evento (protegido)
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

// Exporta inscritos do evento para Excel
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

      registrarAdminLog(req, {
        acao: "exportar_excel_inscritos",
        detalhes: { total: inscritos.length },
      });

      return exportarPlanilha({
        linhas: inscritos,
        aba: "Inscritos",
        arquivo: "inscritos-cafe-empresarios.xlsx",
        res,
        colunas: [
          { header: "ID", key: "id", width: 8 },
          { header: "Email", key: "email", width: 32 },
          { header: "Nome", key: "nome", width: 28 },
          { header: "Empresa", key: "empresa", width: 28 },
          { header: "WhatsApp", key: "whatsapp", width: 18 },
          { header: "Data da Inscrição", key: "data_inscricao", width: 22 },
        ],
      });
    } catch (erro) {
      console.error("Erro ao exportar Excel de inscritos:", erro);
      return res.status(500).json({
        sucesso: false,
        erros: ["Erro interno ao exportar o Excel."],
      });
    }
  })
);

// --- Prospecção (lista de interesse em futuros eventos) ---

// Cria cadastro de prospecção (público)
rotas.post("/prospeccao", (req, res) => {
  const { email, nome, empresa, whatsapp } = req.body || {};
  const erros = validarCadastro({ email, nome, empresa, whatsapp });

  if (erros.length > 0) {
    return res.status(400).json({ sucesso: false, erros });
  }

  try {
    const stmt = db.prepare(`
      INSERT INTO prospeccao (email, nome, empresa, whatsapp)
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
      mensagem: "Cadastro de interesse realizado com sucesso.",
    });
  } catch (erro) {
    console.error("Erro ao salvar prospecção:", erro);
    return res.status(500).json({
      sucesso: false,
      erros: ["Erro interno ao salvar o cadastro."],
    });
  }
});

// Lista cadastros de prospecção (protegido)
rotas.get(
  "/prospeccao",
  comAuth((req, res) => {
    try {
      const leads = db
        .prepare(
          `SELECT id, email, nome, empresa, whatsapp, data_cadastro
           FROM prospeccao
           ORDER BY datetime(data_cadastro) DESC`
        )
        .all();

      registrarAdminLog(req, {
        acao: "listar_prospeccao",
        detalhes: { total: leads.length },
      });

      return res.json({ sucesso: true, dados: leads });
    } catch (erro) {
      console.error("Erro ao listar prospecção:", erro);
      return res.status(500).json({
        sucesso: false,
        erros: ["Erro interno ao listar a prospecção."],
      });
    }
  })
);

// Exporta prospecção para Excel
rotas.get(
  "/prospeccao/export",
  comAuth(async (req, res) => {
    try {
      const leads = db
        .prepare(
          `SELECT id, email, nome, empresa, whatsapp, data_cadastro
           FROM prospeccao
           ORDER BY datetime(data_cadastro) DESC`
        )
        .all();

      registrarAdminLog(req, {
        acao: "exportar_excel_prospeccao",
        detalhes: { total: leads.length },
      });

      return exportarPlanilha({
        linhas: leads,
        aba: "Prospecção",
        arquivo: "prospeccao-cafe-empresarios.xlsx",
        res,
        colunas: [
          { header: "ID", key: "id", width: 8 },
          { header: "Email", key: "email", width: 32 },
          { header: "Nome", key: "nome", width: 28 },
          { header: "Empresa", key: "empresa", width: 28 },
          { header: "WhatsApp", key: "whatsapp", width: 18 },
          { header: "Data do Cadastro", key: "data_cadastro", width: 22 },
        ],
      });
    } catch (erro) {
      console.error("Erro ao exportar Excel de prospecção:", erro);
      return res.status(500).json({
        sucesso: false,
        erros: ["Erro interno ao exportar o Excel."],
      });
    }
  })
);

module.exports = rotas;
