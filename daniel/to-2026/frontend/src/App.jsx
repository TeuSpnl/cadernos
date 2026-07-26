import { Routes, Route } from "react-router-dom";
import FormularioInscricao from "./pages/FormularioInscricao.jsx";
import Admin from "./pages/Admin.jsx";

// Rotas: formulário público e painel administrativo
export default function App() {
  return (
    <Routes>
      <Route path="/" element={<FormularioInscricao />} />
      <Route path="/admin" element={<Admin />} />
    </Routes>
  );
}
