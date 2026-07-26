# Feira Missionaria — Sistema de Vendas de Camisetas

Stack: **Django** + **PostgreSQL** + **HTMX** + **Alpine.js**

A landing page (HTML/CSS) foi preservada e integrada via template tags + HTMX.
Pasta do projeto: `feira/`.

## Subir o ambiente

```bash
cd feira
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Dev local usa SQLite automaticamente (sem DB_* no .env)
cp .env.example .env   # opcional — preencha DB_* para PostgreSQL

python manage.py migrate
python manage.py seed_camisetas
python manage.py createsuperuser   # informe nome completo + senha
python manage.py runserver
```

Abra: http://127.0.0.1:8000/

## Rotas principais

| Rota | Quem | Descricao |
|------|------|-----------|
| `/` | todos | Landing (vitrine) |
| `/cadastrar/` `/entrar/` | publico | Auth simplificada |
| `/carrinho/` | cliente logado | Itens + checkout |
| `/meus-pedidos/` | cliente logado | Historico |
| `/painel/` | **admin only** | Visao geral |
| `/painel/clientes/` | **admin only** | Total gasto por cliente |
| `/painel/pedidos/` | **admin only** | Tabela + status HTMX |

Clientes comuns em `/painel/*` recebem **403**.

## Snippet HTMX — botao Adicionar ao Carrinho

```html
<form
  hx-post="{% url 'loja:adicionar_carrinho' camiseta.slug %}"
  hx-target="#cart-badge"
  hx-swap="outerHTML"
>
  <select name="tamanho">...</select>
  <input type="number" name="quantidade" value="1" />
  <button type="submit" class="btn-buy">Adicionar ao carrinho</button>
</form>
```

O CSRF e enviado globalmente via `hx-headers` no `<body>` (`templates/base.html`).
Sem login, a view responde com `HX-Redirect` para `/entrar/`.

## Status do pedido

- Pagamento: `Em espera` | `Pago` | `Cancelado`
- Entrega (paralelo): `Nao entregue` | `Entregue`

Admin atualiza os selects em `/painel/pedidos/` — POST HTMX troca so a linha da tabela.

## PostgreSQL

No `.env`:

```
DB_NAME=feira_missionaria
DB_USER=postgres
DB_PASSWORD=...
DB_HOST=localhost
DB_PORT=5432
```

Sem `DB_NAME`, o Django usa `db.sqlite3` (ok para desenvolvimento).
