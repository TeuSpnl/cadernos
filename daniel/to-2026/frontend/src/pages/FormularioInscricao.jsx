import { useState } from "react";
import ProgressBar from "../components/ProgressBar.jsx";
import CampoTexto from "../components/CampoTexto.jsx";
import { useStepAnimation } from "../hooks/useStepAnimation.js";
import {
  aplicarMascaraWhatsApp,
  validarEmail,
  validarWhatsApp,
} from "../utils/validacao.js";
import { apiUrl } from "../utils/api.js";
import "../styles/form.css";

const TOTAL_PASSOS = 5;

// Texto de consentimento LGPD (passo final)
const TEXTO_LGPD =
  "Autorizo o uso dos meus dados (nome, nome da empresa, telefone e e-mail) para contato sobre este e futuros eventos e para o envio de conteúdos relevantes. Posso solicitar exclusão a qualquer momento.";

const estadoInicial = {
  email: "",
  nome: "",
  empresa: "",
  whatsapp: "",
};

export default function FormularioInscricao() {
  const [passo, setPasso] = useState(0);
  const [dados, setDados] = useState(estadoInicial);
  const [erro, setErro] = useState("");
  const [consentimento, setConsentimento] = useState(false);
  const [enviando, setEnviando] = useState(false);
  const [concluido, setConcluido] = useState(false);
  const animKey = useStepAnimation(passo);

  function atualizarCampo(campo, valor) {
    setDados((prev) => ({ ...prev, [campo]: valor }));
    setErro("");
  }

  function validarPassoAtual() {
    if (passo === 0) {
      if (!validarEmail(dados.email)) {
        setErro("Informe um e-mail válido.");
        return false;
      }
    }
    if (passo === 1) {
      if (dados.nome.trim().length < 2) {
        setErro("Informe o nome completo.");
        return false;
      }
    }
    if (passo === 2) {
      if (dados.empresa.trim().length < 1) {
        setErro("Informe o nome da empresa.");
        return false;
      }
    }
    if (passo === 3) {
      if (!validarWhatsApp(dados.whatsapp)) {
        setErro("Informe um WhatsApp válido com DDD.");
        return false;
      }
    }
    if (passo === 4) {
      if (!consentimento) {
        setErro("É necessário autorizar o uso dos dados para concluir.");
        return false;
      }
    }
    return true;
  }

  function avancar() {
    if (!validarPassoAtual()) return;
    if (passo < TOTAL_PASSOS - 1) {
      setErro("");
      setPasso((p) => p + 1);
    }
  }

  function voltar() {
    setErro("");
    setPasso((p) => Math.max(0, p - 1));
  }

  async function enviarInscricao() {
    if (!validarPassoAtual()) return;

    setEnviando(true);
    setErro("");

    try {
      const resposta = await fetch(apiUrl("/inscricoes"), {
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
        setErro(json.erros?.[0] || "Não foi possível concluir a inscrição.");
        return;
      }

      setConcluido(true);
    } catch {
      setErro("Falha de conexão. Por favor, avise o responsável pelo evento.");
    } finally {
      setEnviando(false);
    }
  }

  // Tela de sucesso após inscrição
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
            <div className="step success-panel" key="ok">
              <h2>Inscrição confirmada</h2>
              <p>
                Obrigado, {dados.nome.split(" ")[0]}! Nos vemos na{" "}
                <strong>segunda que vem, no auditório da ACE, às 19h!</strong>. Em breve te
                mandaremos novidades — bom café e boas vendas.
              </p>
            </div>
          </main>
        </div>
      </div>
    );
  }

  return (
    <div className="form-shell">
      {/* Coluna única: título e pergunta alinhados */}
      <div className="form-column">
        <header className="form-header">
          <h1 className="form-brand">
            Café com <span>Empresários</span>
          </h1>
          <ProgressBar atual={passo + 1} total={TOTAL_PASSOS} />
        </header>

        <main className="form-main">
          <div className="step" key={animKey}>
            {passo === 0 && (
              <>
                <span className="step__label">{passo + 1}</span>
                <h2 className="step__question">Qual é o seu e-mail?</h2>
                <CampoTexto
                  id="email"
                  type="email"
                  value={dados.email}
                  onChange={(v) => atualizarCampo("email", v)}
                  onEnter={avancar}
                  placeholder="seu@email.com"
                  autoComplete="email"
                  inputMode="email"
                  error={erro}
                />
                <div className="step-actions">
                  <button type="button" className="btn btn--primary" onClick={avancar}>
                    Continuar
                  </button>
                </div>
              </>
            )}

            {passo === 1 && (
              <>
                <span className="step__label">{passo + 1}</span>
                <h2 className="step__question">Qual é o seu nome completo?</h2>
                <CampoTexto
                  id="nome"
                  value={dados.nome}
                  onChange={(v) => atualizarCampo("nome", v)}
                  onEnter={avancar}
                  placeholder="Nome e sobrenome"
                  autoComplete="name"
                  error={erro}
                />
                <div className="step-actions">
                  <button type="button" className="btn btn--secondary" onClick={voltar}>
                    Voltar
                  </button>
                  <button type="button" className="btn btn--primary" onClick={avancar}>
                    Continuar
                  </button>
                </div>
              </>
            )}

            {passo === 2 && (
              <>
                <span className="step__label">{passo + 1}</span>
                <h2 className="step__question">Qual é o nome da sua empresa?</h2>
                <CampoTexto
                  id="empresa"
                  value={dados.empresa}
                  onChange={(v) => atualizarCampo("empresa", v)}
                  onEnter={avancar}
                  placeholder="Razão social ou nome fantasia"
                  autoComplete="organization"
                  error={erro}
                />
                <div className="step-actions">
                  <button type="button" className="btn btn--secondary" onClick={voltar}>
                    Voltar
                  </button>
                  <button type="button" className="btn btn--primary" onClick={avancar}>
                    Continuar
                  </button>
                </div>
              </>
            )}

            {passo === 3 && (
              <>
                <span className="step__label">{passo + 1}</span>
                <h2 className="step__question">Qual é o seu WhatsApp?</h2>
                <p className="step__hint">Com DDD, por favor.</p>
                <CampoTexto
                  id="whatsapp"
                  type="tel"
                  value={dados.whatsapp}
                  onChange={(v) =>
                    atualizarCampo("whatsapp", aplicarMascaraWhatsApp(v))
                  }
                  onEnter={avancar}
                  placeholder="(33) 99999-9999"
                  autoComplete="tel"
                  inputMode="tel"
                  error={erro}
                />
                <div className="step-actions">
                  <button type="button" className="btn btn--secondary" onClick={voltar}>
                    Voltar
                  </button>
                  <button type="button" className="btn btn--primary" onClick={avancar}>
                    Continuar
                  </button>
                </div>
              </>
            )}

            {passo === 4 && (
              <>
                <span className="step__label">{passo + 1}</span>
                <h2 className="step__question">Quase lá — autorização LGPD</h2>
                <p className="consent">{TEXTO_LGPD}</p>
                <label className="checkbox-row">
                  <input
                    type="checkbox"
                    checked={consentimento}
                    onChange={(e) => {
                      setConsentimento(e.target.checked);
                      setErro("");
                    }}
                  />
                  <span>Li e autorizo o uso dos meus dados conforme o texto acima.</span>
                </label>
                {erro ? (
                  <p className="field-error" role="alert">
                    {erro}
                  </p>
                ) : null}
                <div className="step-actions">
                  <button
                    type="button"
                    className="btn btn--secondary"
                    onClick={voltar}
                    disabled={enviando}
                  >
                    Voltar
                  </button>
                  <button
                    type="button"
                    className="btn btn--primary"
                    onClick={enviarInscricao}
                    disabled={enviando}
                  >
                    {enviando ? "Enviando…" : "Inscrever-se"}
                  </button>
                </div>
              </>
            )}
          </div>
        </main>
      </div>
    </div>
  );
}
