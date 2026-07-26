# Café com Empresários — Inscrição (React + Express + SQLite)

Sistema de inscrição em página única (estilo Typeform) para o evento **Café com Empresários**, com API Node.js/Express, banco SQLite local e painel administrativo com exportação para Excel.

## Estrutura

```
daniel/
??? backend/                 # API Express + SQLite
?   ??? server.js
?   ??? database.js
?   ??? package.json
?   ??? database.sqlite      # criado automaticamente ao iniciar
??? frontend/                # React (Vite)
?   ??? index.html
?   ??? package.json
?   ??? vite.config.js
?   ??? src/
?       ??? App.jsx
?       ??? main.jsx
?       ??? pages/
?       ?   ??? FormularioInscricao.jsx
?       ?   ??? Admin.jsx
?       ??? components/
?       ??? hooks/
?       ??? styles/
?       ??? utils/
??? README.md
```

## Pré-requisitos

- Node.js **18+** (recomendado 20+)
- npm

## Como rodar localmente

Abra **dois terminais** (API e frontend).

### 1. Backend (porta 3001)

```bash
cd backend
npm install
npm run dev
```

A API sobe em `http://localhost:3001` e cria o arquivo `database.sqlite` com a tabela `inscricoes`.

### 2. Frontend (porta 5173)

```bash
cd frontend
npm install
npm run dev
```

Abra `http://localhost:5173/to-2026` no navegador.

O Vite encaminha `/api` para o backend via proxy — não precisa configurar CORS no dia a dia de desenvolvimento.

Frontend e API sob o prefixo `/to-2026` (`BrowserRouter basename` + `app.use('/to-2026/api', rotas)`).

## Rotas da aplicação

| Rota | Descrição |
|------|-----------|
| `/to-2026/` | Formulário de inscrição (uma pergunta por vez) |
| `/to-2026/admin` | Painel com tabela de inscritos + **Exportar para Excel** |

## Endpoints da API

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/to-2026/api/inscricoes` | Cria inscrição (`email`, `nome`, `empresa`, `whatsapp`) |
| `GET` | `/to-2026/api/inscricoes` | Lista todas as inscrições |
| `GET` | `/to-2026/api/inscricoes/export` | Download do arquivo `.xlsx` (via exceljs) |

## Fluxo do formulário

1. E-mail (validação básica)
2. Nome completo
3. Nome da empresa
4. WhatsApp (máscara `(XX) XXXXX-XXXX`)
5. Consentimento LGPD + botão **Inscrever-se**

## Design

- Fundo `#161616`
- Texto `#FFFFFF`
- Destaques / progresso / botões primários `#F3C044` (texto preto nos botões)
- Botão **Voltar** em cinza claro (sem amarelo)

## Scripts úteis

```bash
# Backend em produção
cd backend && npm start

# Build do frontend
cd frontend && npm run build
cd frontend && npm run preview
```


## Admin (login)

O painel `/admin` exige login. Não há cadastro pela interface — o usuário é criado no terminal:

```bash
cd backend
npm run criar-admin -- seu_usuario
```

A senha é pedida de forma oculta (não aparece no terminal nem no histórico do shell). Se o usuário já existir, a senha é atualizada e as sessões antigas são encerradas.

## Segurança do admin

- Senhas no banco: **bcrypt** com salt aleatório e custo 12 — nunca texto puro.
- Sessões: o navegador guarda o token; no SQLite fica só o **SHA-256** desse token.
- Listagem e exportação Excel exigem autenticação.


## Logs de auditoria (admin)

Sem tela. Cada ação relevante do painel é gravada em:

- Tabela SQLite `admin_logs`
- Arquivo `backend/logs/admin.log` (útil com `tail -f`)

Ações registradas: `login_sucesso`, `login_falha`, `logout`, `acesso_painel`, `listar_inscritos`, `exportar_excel`, `acesso_negado`.
Senhas **nunca** entram no log.

## Observações

- O arquivo `database.sqlite` fica em `backend/` e entra no `.gitignore`.
- Em produção, sirva o `frontend/dist` atrás de um proxy reverso apontando `/api` para o Express, ou configure a URL da API via variável de ambiente se preferir.
