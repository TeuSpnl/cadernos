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

// Painel administrativo: login, lista inscritos e exporta Excel

function rotuloInscricoes(total) {
  // 0 ou 1 → inscrição; 2+ → inscrições
  return total <= 1 ? "inscrição" : "inscrições";
}

export default function Admin() {
  const [autenticado, setAutenticado] = useState(false);
  const [checandoSessao, setChecandoSessao] = useState(true);
  const [usuario, setUsuario] = useState("");
  const [loginUsuario, setLoginUsuario] = useState("");
  const [loginSenha, setLoginSenha] = useState("");
  const [loginErro, setLoginErro] = useState("");
  const [entrando, setEntrando] = useState(false);

  const [inscritos, setInscritos] = useState([]);
  const [carregando, setCarregando] = useState(false);
  const [erro, setErro] = useState("");
  const [exportando, setExportando] = useState(false);

  // Valida token salvo ao abrir /to-2026/admin
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
      const resposta = await fetchAutenticado(apiUrl("/inscricoes"));
      if (resposta.status === 401) {
        limparSessao();
        setAutenticado(false);
        throw new Error("Sessão expirada. Faça login novamente.");
      }
      const json = await resposta.json();
      if (!resposta.ok || !json.sucesso) {
        throw new Error(json.erros?.[0] || "Falha ao carregar inscritos.");
      }
      setInscritos(json.dados || []);
    } catch (e) {
      setErro(e.message || "Erro ao carregar dados.");
      setInscritos([]);
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
    setUsuario("");
  }

  async function exportarExcel() {
    setExportando(true);
    setErro("");
    try {
      const resposta = await fetchAutenticado(apiUrl("/inscricoes/export"));
      if (resposta.status === 401) {
        limparSessao();
        setAutenticado(false);
        throw new Error("Sessão expirada. Faça login novamente.");
      }
      if (!resposta.ok) {
        throw new Error("Não foi possível exportar o arquivo.");
      }
      const blob = await resposta.blob();
      // Download do .xlsx no navegador
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "inscritos-cafe-empresarios.xlsx";
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

  // Tela de login — sem cadastro público
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

  return (
    <div className="admin-shell">
      <header className="admin-header">
        <div>
          <h1>Painel de inscritos</h1>
          <p>
            {carregando ? (
              "Carregando…"
            ) : (
              <>
                Total:{" "}
                <span className="admin-count">{inscritos.length}</span>{" "}
                {rotuloInscricoes(inscritos.length)}
                {" · "}
                <span className="admin-user">Olá, {usuario}</span>
              </>
            )}
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
            disabled={exportando || inscritos.length === 0}
          >
            {exportando ? "Exportando…" : "Exportar para Excel"}
          </button>
          <button type="button" className="btn btn--secondary" onClick={sair}>
            Sair
          </button>
        </div>
      </header>

      {erro ? <p className="admin-error">{erro}</p> : null}

      <div className="admin-table-wrap">
        {carregando ? (
          <p className="admin-status">Buscando inscritos no banco…</p>
        ) : inscritos.length === 0 ? (
          <p className="admin-empty">
            Nenhuma inscrição ainda. O café esfria, o banco não.
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
                <th>Data da inscrição</th>
              </tr>
            </thead>
            <tbody>
              {inscritos.map((item) => (
                <tr key={item.id}>
                  <td>{item.id}</td>
                  <td>{item.email}</td>
                  <td>{item.nome}</td>
                  <td>{item.empresa}</td>
                  <td>{item.whatsapp}</td>
                  <td>{item.data_inscricao}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
