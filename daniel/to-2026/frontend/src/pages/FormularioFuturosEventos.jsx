import { useState } from "react";
import {
  aplicarMascaraWhatsApp,
  validarEmail,
  validarWhatsApp,
} from "../utils/validacao.js";
import { apiUrl } from "../utils/api.js";
import "../styles/form.css";

// Texto de consentimento LGPD
const TEXTO_LGPD =
  "Autorizo o uso dos meus dados (nome, nome da empresa, telefone e e-mail) para contato sobre este e futuros eventos e para o envio de conteúdos relevantes. Posso solicitar exclusão a qualquer momento.";

const estadoInicial = {
  email: "",
  nome: "",
  empresa: "",
  whatsapp: "",
};

export default function FormularioFuturosEventos() {
  const [dados, setDados] = useState(estadoInicial);
  const [erros, setErros] = useState({});
  const [consentimento, setConsentimento] = useState(false);
  const [enviando, setEnviando] = useState(false);
  const [concluido, setConcluido] = useState(false);

  function atualizarCampo(campo, valor) {
    setDados((prev) => ({ ...prev, [campo]: valor }));
    setErros((prev) => ({ ...prev, [campo]: "", geral: "" }));
  }

  function validarFormulario() {
    const novosErros = {};

    if (!validarEmail(dados.email)) {
      novosErros.email = "Informe um e-mail válido.";
    }
    if (dados.nome.trim().length < 2) {
      novosErros.nome = "Informe o nome completo.";
    }
    if (dados.empresa.trim().length < 1) {
      novosErros.empresa = "Informe o nome da empresa.";
    }
    if (!validarWhatsApp(dados.whatsapp)) {
      novosErros.whatsapp = "Informe um WhatsApp válido com DDD.";
    }
    if (!consentimento) {
      novosErros.geral = "É necessário autorizar o uso dos dados para continuar.";
    }

    setErros(novosErros);
    return Object.keys(novosErros).length === 0;
  }

  async function enviar(e) {
    e.preventDefault();
    if (!validarFormulario()) return;

    setEnviando(true);
    setErros({});

    try {
      // Salva na tabela de prospecção — não misturar com inscritos do evento
      const resposta = await fetch(apiUrl("/prospeccao"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          email: dados.email.trim(),
          nome: dados.nome.trim(),
          empresa: dados.empresa.trim(),
          whatsapp: dados.whatsapp.trim(),
        }),
      });

      const json = await resposta.json();

      if (!resposta.ok || !json.sucesso) {
        setErros({
          geral: json.erros?.[0] || "Não foi possível enviar seus dados.",
        });
        return;
      }

      setConcluido(true);
    } catch {
      setErros({
        geral: "Falha de conexão. Por favor, avise o responsável pelo evento.",
      });
    } finally {
      setEnviando(false);
    }
  }

  if (concluido) {
    return (
      <div className="form-shell">
        <div className="form-column">
          <header className="form-header">
            <h1 className="form-brand">
              Café com <span>Empresários</span>
            </h1>
          </header>
          <main className="form-main">
            <div className="step success-panel">
              <h2>Dados recebidos!</h2>
              <p>
                Obrigado, {dados.nome.split(" ")[0]}! Em breve entraremos em
                contato para avisar sobre os próximos eventos.
              </p>
            </div>
          </main>
        </div>
      </div>
    );
  }

  return (
    <div className="form-shell">
      <div className="form-column form-column--single">
        <header className="form-header">
          <h1 className="form-brand">
            Café com <span>Empresários</span>
          </h1>
        </header>

        <main className="form-main form-main--single">
          <form className="step single-form" onSubmit={enviar} noValidate>
            <h2 className="step__question single-form__title">
              Deixe seus dados para te avisarmos de outros eventos
            </h2>
            <p className="single-form__intro">
              As inscrições para esta edição estão encerradas. Deixe seus dados
              abaixo e avisamos quando surgirem novos eventos e oportunidades
              de networking.
            </p>

            <div className="single-form__fields">
              <label className="single-form__label" htmlFor="email">
                E-mail
              </label>
              <input
                id="email"
                className={`field${erros.email ? " field--error" : ""}`}
                type="email"
                value={dados.email}
                onChange={(e) => atualizarCampo("email", e.target.value)}
                placeholder="seu@email.com"
                autoComplete="email"
              />
              {erros.email ? (
                <p className="field-error">{erros.email}</p>
              ) : null}

              <label className="single-form__label" htmlFor="nome">
                Nome completo
              </label>
              <input
                id="nome"
                className={`field${erros.nome ? " field--error" : ""}`}
                value={dados.nome}
                onChange={(e) => atualizarCampo("nome", e.target.value)}
                placeholder="Nome e sobrenome"
                autoComplete="name"
              />
              {erros.nome ? <p className="field-error">{erros.nome}</p> : null}

              <label className="single-form__label" htmlFor="empresa">
                Empresa
              </label>
              <input
                id="empresa"
                className={`field${erros.empresa ? " field--error" : ""}`}
                value={dados.empresa}
                onChange={(e) => atualizarCampo("empresa", e.target.value)}
                placeholder="Razão social ou nome fantasia"
                autoComplete="organization"
              />
              {erros.empresa ? (
                <p className="field-error">{erros.empresa}</p>
              ) : null}

              <label className="single-form__label" htmlFor="whatsapp">
                WhatsApp
              </label>
              <input
                id="whatsapp"
                className={`field${erros.whatsapp ? " field--error" : ""}`}
                type="tel"
                value={dados.whatsapp}
                onChange={(e) =>
                  atualizarCampo(
                    "whatsapp",
                    aplicarMascaraWhatsApp(e.target.value)
                  )
                }
                placeholder="(33) 99999-9999"
                autoComplete="tel"
                inputMode="tel"
              />
              {erros.whatsapp ? (
                <p className="field-error">{erros.whatsapp}</p>
              ) : null}
            </div>

            <p className="consent single-form__consent">{TEXTO_LGPD}</p>
            <label className="checkbox-row">
              <input
                type="checkbox"
                checked={consentimento}
                onChange={(e) => {
                  setConsentimento(e.target.checked);
                  setErros((prev) => ({ ...prev, geral: "" }));
                }}
              />
              <span>Li e autorizo o uso dos meus dados conforme o texto acima.</span>
            </label>

            {erros.geral ? (
              <p className="field-error" role="alert">
                {erros.geral}
              </p>
            ) : null}

            <div className="step-actions">
              <button
                type="submit"
                className="btn btn--primary"
                disabled={enviando}
              >
                {enviando ? "Enviando..." : "Enviar meus dados"}
              </button>
            </div>
          </form>
        </main>
      </div>
    </div>
  );
}
