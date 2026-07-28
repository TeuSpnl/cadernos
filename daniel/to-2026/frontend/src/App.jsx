import { useEffect, useState } from "react";
import { Routes, Route } from "react-router-dom";
import FormularioInscricao from "./pages/FormularioInscricao.jsx";
import FormularioFuturosEventos from "./pages/FormularioFuturosEventos.jsx";
import Admin from "./pages/Admin.jsx";
import { apiUrl } from "./utils/api.js";
import "./styles/form.css";

/**
 * Página pública: escolhe o formulário conforme o status da API.
 * true  → inscrição (tabela inscricoes)
 * false → prospecção (tabela prospeccao)
 */
function PaginaPublica() {
  const [carregando, setCarregando] = useState(true);
  const [inscricoesAbertas, setInscricoesAbertas] = useState(true);
  const [erro, setErro] = useState("");

  useEffect(() => {
    async function carregarStatus() {
      try {
        const resposta = await fetch(apiUrl("/status"));
        const json = await resposta.json();
        if (!resposta.ok || !json.sucesso) {
          throw new Error("Não foi possível verificar o status das inscrições.");
        }
        setInscricoesAbertas(Boolean(json.inscricoesAbertas));
      } catch {
        // Sem API: assume fechado e mostra prospecção (mais seguro)
        setInscricoesAbertas(false);
        setErro(
          "Não foi possível confirmar se as inscrições estão abertas. Exibindo o formulário de interesse."
        );
      } finally {
        setCarregando(false);
      }
    }

    carregarStatus();
  }, []);

  if (carregando) {
    return (
      <div className="form-shell">
        <div className="form-column">
          <header className="form-header">
            <h1 className="form-brand">
              Café com <span>Empresários</span>
            </h1>
          </header>
          <main className="form-main">
            <p className="single-form__intro">Carregando…</p>
          </main>
        </div>
      </div>
    );
  }

  return (
    <>
      {erro ? (
        <p
          className="field-error"
          style={{ textAlign: "center", margin: "1rem" }}
          role="status"
        >
          {erro}
        </p>
      ) : null}
      {inscricoesAbertas ? (
        <FormularioInscricao />
      ) : (
        <FormularioFuturosEventos />
      )}
    </>
  );
}

// Rotas: formulário público (modo por status) e painel admin
export default function App() {
  return (
    <Routes>
      <Route path="/" element={<PaginaPublica />} />
      <Route path="/admin" element={<Admin />} />
    </Routes>
  );
}
