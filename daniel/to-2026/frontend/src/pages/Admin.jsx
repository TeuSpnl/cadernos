import { useCallback, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import {
  fetchAutenticado,
  limparSessao,
  obterToken,
  obterUsuario,
  salvarSessao,
} from "../utils/auth.js";
import { apiUrl } from "../utils/api.js";
import "../styles/form.css";
import "../styles/admin.css";

// Painel: inscritos do evento + prospecção (tabelas separadas)

function rotuloContagem(total, singular, plural) {
  return total <= 1 ? singular : plural;
}

export default function Admin() {
  const [autenticado, setAutenticado] = useState(false);
  const [checandoSessao, setChecandoSessao] = useState(true);
  const [usuario, setUsuario] = useState("");
  const [loginUsuario, setLoginUsuario] = useState("");
  const [loginSenha, setLoginSenha] = useState("");
  const [loginErro, setLoginErro] = useState("");
  const [entrando, setEntrando] = useState(false);

  // Aba ativa: inscritos (evento) ou prospeccao
  const [aba, setAba] = useState("inscritos");
  const [inscritos, setInscritos] = useState([]);
  const [prospeccao, setProspeccao] = useState([]);
  const [carregando, setCarregando] = useState(false);
  const [erro, setErro] = useState("");
  const [exportando, setExportando] = useState(false);
  const [inscricoesAbertas, setInscricoesAbertas] = useState(true);
  const [alternandoStatus, setAlternandoStatus] = useState(false);

  useEffect(() => {
    async function verificarSessao() {
      const token = obterToken();
      if (!token) {
        setChecandoSessao(false);
        return;
      }

      try {
        const resposta = await fetchAutenticado(apiUrl("/admin/me"));
        if (!resposta.ok) {
          limparSessao();
          setAutenticado(false);
        } else {
          const json = await resposta.json();
          setUsuario(json.usuario || obterUsuario());
          setAutenticado(true);
        }
      } catch {
        limparSessao();
        setAutenticado(false);
      } finally {
        setChecandoSessao(false);
      }
    }

    verificarSessao();
  }, []);

  const carregar = useCallback(async () => {
    setCarregando(true);
    setErro("");
    try {
      const [resInscritos, resProspeccao, resStatus] = await Promise.all([
        fetchAutenticado(apiUrl("/inscricoes")),
        fetchAutenticado(apiUrl("/prospeccao")),
        fetch(apiUrl("/status")),
      ]);

      if (resInscritos.status === 401 || resProspeccao.status === 401) {
        limparSessao();
        setAutenticado(false);
        throw new Error("Sessão expirada. Faça login novamente.");
      }

      const jsonInscritos = await resInscritos.json();
      const jsonProspeccao = await resProspeccao.json();
      const jsonStatus = await resStatus.json();

      if (!resInscritos.ok || !jsonInscritos.sucesso) {
        throw new Error(
          jsonInscritos.erros?.[0] || "Falha ao carregar inscritos."
        );
      }
      if (!resProspeccao.ok || !jsonProspeccao.sucesso) {
        throw new Error(
          jsonProspeccao.erros?.[0] || "Falha ao carregar prospecção."
        );
      }

      setInscritos(jsonInscritos.dados || []);
      setProspeccao(jsonProspeccao.dados || []);
      if (jsonStatus?.sucesso) {
        setInscricoesAbertas(Boolean(jsonStatus.inscricoesAbertas));
      }
    } catch (e) {
      setErro(e.message || "Erro ao carregar dados.");
      setInscritos([]);
      setProspeccao([]);
    } finally {
      setCarregando(false);
    }
  }, []);

  useEffect(() => {
    if (autenticado) {
      carregar();
    }
  }, [autenticado, carregar]);

  async function fazerLogin(e) {
    e.preventDefault();
    setLoginErro("");
    setEntrando(true);

    try {
      const resposta = await fetch(apiUrl("/admin/login"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          usuario: loginUsuario.trim(),
          senha: loginSenha,
        }),
      });
      const json = await resposta.json();

      if (!resposta.ok || !json.sucesso) {
        setLoginErro(json.erros?.[0] || "Falha no login.");
        return;
      }

      salvarSessao({ token: json.token, usuario: json.usuario });
      setUsuario(json.usuario);
      setLoginSenha("");
      setAutenticado(true);
    } catch {
      setLoginErro("Falha de conexão com a API.");
    } finally {
      setEntrando(false);
    }
  }

  async function sair() {
    try {
      await fetchAutenticado(apiUrl("/admin/logout"), { method: "POST" });
    } catch {
      // Mesmo se a API falhar, limpa a sessão local
    }
    limparSessao();
    setAutenticado(false);
    setInscritos([]);
    setProspeccao([]);
    setUsuario("");
  }

  async function alternarInscricoes() {
    const novoEstado = !inscricoesAbertas;
    const acao = novoEstado
      ? "Reabrir as inscrições do evento?"
      : "Encerrar as inscrições do evento?\n\nO site público passará a mostrar o formulário de interesse (prospecção).";

    if (!window.confirm(acao)) return;

    setAlternandoStatus(true);
    setErro("");
    try {
      const resposta = await fetchAutenticado(apiUrl("/admin/inscricoes-status"), {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ abertas: novoEstado }),
      });

      if (resposta.status === 401) {
        limparSessao();
        setAutenticado(false);
        throw new Error("Sessão expirada. Faça login novamente.");
      }

      const json = await resposta.json();
      if (!resposta.ok || !json.sucesso) {
        throw new Error(json.erros?.[0] || "Não foi possível alterar o status.");
      }

      setInscricoesAbertas(Boolean(json.inscricoesAbertas));
    } catch (e) {
      setErro(e.message || "Erro ao alterar status das inscrições.");
    } finally {
      setAlternandoStatus(false);
    }
  }

  async function exportarExcel() {
    setExportando(true);
    setErro("");
    const ehProspeccao = aba === "prospeccao";
    const endpoint = ehProspeccao ? "/prospeccao/export" : "/inscricoes/export";
    const arquivo = ehProspeccao
      ? "prospeccao-cafe-empresarios.xlsx"
      : "inscritos-cafe-empresarios.xlsx";

    try {
      const resposta = await fetchAutenticado(apiUrl(endpoint));
      if (resposta.status === 401) {
        limparSessao();
        setAutenticado(false);
        throw new Error("Sessão expirada. Faça login novamente.");
      }
      if (!resposta.ok) {
        throw new Error("Não foi possível exportar o arquivo.");
      }
      const blob = await resposta.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = arquivo;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    } catch (e) {
      setErro(e.message || "Erro na exportação.");
    } finally {
      setExportando(false);
    }
  }

  if (checandoSessao) {
    return (
      <div className="admin-shell">
        <p className="admin-status">Verificando sessão…</p>
      </div>
    );
  }

  if (!autenticado) {
    return (
      <div className="admin-shell admin-shell--login">
        <div className="admin-login">
          <h1>Acesso ao painel</h1>
          <p className="admin-login__hint">
            Área restrita. O usuário é criado só pelo terminal.
          </p>
          <form className="admin-login__form" onSubmit={fazerLogin}>
            <label className="admin-login__label" htmlFor="admin-usuario">
              Usuário
            </label>
            <input
              id="admin-usuario"
              className="field"
              value={loginUsuario}
              onChange={(e) => setLoginUsuario(e.target.value)}
              autoComplete="username"
              required
            />
            <label className="admin-login__label" htmlFor="admin-senha">
              Senha
            </label>
            <input
              id="admin-senha"
              className="field"
              type="password"
              value={loginSenha}
              onChange={(e) => setLoginSenha(e.target.value)}
              autoComplete="current-password"
              required
            />
            {loginErro ? (
              <p className="admin-error" role="alert">
                {loginErro}
              </p>
            ) : null}
            <button
              type="submit"
              className="btn btn--primary"
              disabled={entrando}
            >
              {entrando ? "Entrando…" : "Entrar"}
            </button>
          </form>
          <Link to="/" className="admin-login__back">
            Voltar ao formulário
          </Link>
        </div>
      </div>
    );
  }

  const listaAtiva = aba === "prospeccao" ? prospeccao : inscritos;
  const colunaData =
    aba === "prospeccao" ? "data_cadastro" : "data_inscricao";
  const rotuloData =
    aba === "prospeccao" ? "Data do cadastro" : "Data da inscrição";

  return (
    <div className="admin-shell">
      <header className="admin-header">
        <div>
          <h1>Painel administrativo</h1>
          <p>
            {carregando ? (
              "Carregando…"
            ) : (
              <>
                <span className="admin-user">Olá, {usuario}</span>
                {" · "}
                Inscritos:{" "}
                <span className="admin-count">{inscritos.length}</span>
                {" · "}
                Prospecção:{" "}
                <span className="admin-count">{prospeccao.length}</span>
              </>
            )}
          </p>
          <p className="admin-form-status">
            Formulário de inscrição:{" "}
            <span
              className={
                inscricoesAbertas
                  ? "admin-badge admin-badge--open"
                  : "admin-badge admin-badge--closed"
              }
            >
              {inscricoesAbertas ? "Aberto" : "Encerrado"}
            </span>
          </p>
        </div>
        <div className="admin-actions">
          <Link
            to="/"
            className="btn btn--secondary"
            style={{ textDecoration: "none" }}
          >
            Ver formulário
          </Link>
          <button
            type="button"
            className={
              inscricoesAbertas ? "btn btn--danger" : "btn btn--primary"
            }
            onClick={alternarInscricoes}
            disabled={alternandoStatus}
          >
            {alternandoStatus
              ? "Alterando…"
              : inscricoesAbertas
                ? "Encerrar inscrições"
                : "Reabrir inscrições"}
          </button>
          <button
            type="button"
            className="btn btn--secondary"
            onClick={carregar}
            disabled={carregando}
          >
            Atualizar
          </button>
          <button
            type="button"
            className="btn btn--primary"
            onClick={exportarExcel}
            disabled={exportando || listaAtiva.length === 0}
          >
            {exportando ? "Exportando…" : "Exportar para Excel"}
          </button>
          <button type="button" className="btn btn--secondary" onClick={sair}>
            Sair
          </button>
        </div>
      </header>

      {/* Abas: inscritos do evento x prospecção */}
      <div className="admin-tabs" role="tablist" aria-label="Tipo de cadastro">
        <button
          type="button"
          role="tab"
          aria-selected={aba === "prospeccao"}
          className={`admin-tab${aba === "prospeccao" ? " admin-tab--active" : ""}`}
          onClick={() => setAba("prospeccao")}
        >
          Prospecção ({prospeccao.length})
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={aba === "inscritos"}
          className={`admin-tab${aba === "inscritos" ? " admin-tab--active" : ""}`}
          onClick={() => setAba("inscritos")}
        >
          Inscritos do evento ({inscritos.length})
        </button>
      </div>

      <p className="admin-tab-hint">
        {aba === "prospeccao"
          ? "Quem deixou dados para ser avisado de outros eventos."
          : "Quem se inscreveu no Café com Empresários desta edição."}
        {" "}
        Total nesta aba:{" "}
        <span className="admin-count">{listaAtiva.length}</span>{" "}
        {rotuloContagem(
          listaAtiva.length,
          aba === "prospeccao" ? "cadastro" : "inscrição",
          aba === "prospeccao" ? "cadastros" : "inscrições"
        )}
        .
      </p>

      {erro ? <p className="admin-error">{erro}</p> : null}

      <div className="admin-table-wrap">
        {carregando ? (
          <p className="admin-status">Buscando dados no banco…</p>
        ) : listaAtiva.length === 0 ? (
          <p className="admin-empty">
            {aba === "prospeccao"
              ? "Nenhum cadastro de prospecção ainda."
              : "Nenhuma inscrição do evento nesta lista."}
          </p>
        ) : (
          <table className="admin-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>E-mail</th>
                <th>Nome</th>
                <th>Empresa</th>
                <th>WhatsApp</th>
                <th>{rotuloData}</th>
              </tr>
            </thead>
            <tbody>
              {listaAtiva.map((item) => (
                <tr key={`${aba}-${item.id}`}>
                  <td>{item.id}</td>
                  <td>{item.email}</td>
                  <td>{item.nome}</td>
                  <td>{item.empresa}</td>
                  <td>{item.whatsapp}</td>
                  <td>{item[colunaData]}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
