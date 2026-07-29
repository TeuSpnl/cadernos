"""
Views da loja: landing, auth, carrinho, pedidos do cliente e painel admin.

HTMX: respostas parciais para badge do carrinho e linhas de status.
Segurança: dashboards e mutações de status → administrador_required (403).
"""

from __future__ import annotations

from django.contrib import messages
from django.contrib.auth import login, logout
from django.contrib.auth.decorators import login_required
from django.db.models import Count, Q, Sum
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from .decorators import administrador_required, htmx_login_required
from .exports import gerar_excel_painel, nome_arquivo_excel
from .forms import AdicionarCarrinhoForm, AtualizarStatusForm, CadastroForm, LoginForm
from .models import Camiseta, ItemCarrinho, ItemPedido, Pedido, Usuario
from .services import (
    adicionar_ao_carrinho,
    aplicar_carrinho_pendente,
    atualizar_quantidade_item,
    finalizar_compra,
    guardar_carrinho_pendente,
    obter_ou_criar_carrinho,
)
from urllib.parse import quote
from django.urls import reverse


# ---------------------------------------------------------------------------
# Landing (HTML/CSS existentes — só pluga HTMX nos botões)
# ---------------------------------------------------------------------------


@require_GET
def landing(request: HttpRequest) -> HttpResponse:
    """Vitrine: adultas (R$ 50) e infantis (R$ 40), mesmos modelos."""
    # Se logou e havia item pendente, aplica e permanece na landing (comprar mais)
    if request.user.is_authenticated and aplicar_carrinho_pendente(request):
        messages.success(request, "Item adicionado ao carrinho.")

    qs = Camiseta.objects.filter(ativo=True)
    # Ordem fixa na grade: Verde (destaque) | Branca | Bege
    ordem = ["verde-militar", "branca-classic", "bege-areia"]
    adultos = list(qs.filter(categoria=Camiseta.Categoria.ADULTO))
    infantis = list(qs.filter(categoria=Camiseta.Categoria.INFANTIL))

    def _ordenar(lista):
        mapa = {c.slug.replace("-infantil", ""): c for c in lista}
        # Preferencia pela ordem dos modelos; sobras no fim
        ordenados = [mapa[s] for s in ordem if s in mapa]
        resto = [c for c in lista if c not in ordenados]
        return ordenados + resto

    return render(
        request,
        "loja/landing.html",
        {
            "camisetas_adulto": _ordenar(adultos),
            "camisetas_infantil": _ordenar(infantis),
        },
    )


# ---------------------------------------------------------------------------
# Autenticação (cadastro simplificado)
# ---------------------------------------------------------------------------


def _apos_autenticar(request: HttpRequest, *, mensagem: str | None = None) -> HttpResponse:
    """Cria carrinho, aplica item pendente da sessao e volta a landing."""
    obter_ou_criar_carrinho(request.user)
    aplicou = aplicar_carrinho_pendente(request)
    if mensagem:
        messages.success(request, mensagem)
    if aplicou:
        messages.success(request, "Item adicionado ao carrinho.")
    # Sempre landing apos login/cadastro com intencao de compra — objetivo: comprar mais
    next_url = request.GET.get("next") or "loja:landing"
    return redirect(next_url)


@require_http_methods(["GET", "POST"])
def cadastro(request: HttpRequest) -> HttpResponse:
    if request.user.is_authenticated:
        return redirect("loja:landing")

    form = CadastroForm(request.POST or None)
    if request.method == "POST" and form.is_valid():
        user = form.save()
        login(request, user)
        obter_ou_criar_carrinho(request.user)
        aplicou = aplicar_carrinho_pendente(request)
        messages.success(request, "Conta criada. Bem-vindo(a)!")
        if aplicou:
            messages.success(request, "Item adicionado ao carrinho.")
        destino = reverse("loja:landing")
        # HTMX: HX-Redirect evita swap do HTML da landing dentro do form
        if getattr(request, "htmx", False):
            response = HttpResponse(status=204)
            response["HX-Redirect"] = destino
            return response
        return redirect(destino)

    # Erros: devolve so o formulario (HTMX) — dados preenchidos permanecem
    if request.method == "POST" and getattr(request, "htmx", False):
        return render(
            request,
            "registration/partials/cadastro_form.html",
            {"form": form},
        )
    return render(request, "registration/cadastro.html", {"form": form})


@require_http_methods(["GET", "POST"])
def entrar(request: HttpRequest) -> HttpResponse:
    if request.user.is_authenticated:
        return redirect("loja:landing")

    form = LoginForm(request, data=request.POST or None)
    if request.method == "POST" and form.is_valid():
        login(request, form.get_user())
        return _apos_autenticar(request)
    return render(request, "registration/login.html", {"form": form})


@require_POST
def sair(request: HttpRequest) -> HttpResponse:
    logout(request)
    return redirect("loja:landing")


# ---------------------------------------------------------------------------
# Carrinho (HTMX)
# ---------------------------------------------------------------------------


@require_POST
def intencao_carrinho(request: HttpRequest, slug: str) -> HttpResponse:
    """
    Visitante clicou em adicionar: guarda slug/tamanho/qtd na sessao
    e manda para o login (apos autenticar, o item entra no carrinho).
    """
    camiseta = get_object_or_404(Camiseta, slug=slug, ativo=True)
    form = AdicionarCarrinhoForm(request.POST, camiseta=camiseta)
    if form.is_valid():
        tamanho = form.cleaned_data["tamanho"]
        quantidade = form.cleaned_data["quantidade"]
    else:
        tamanho = camiseta.tamanho_padrao
        quantidade = 1

    # Se ja estiver logado (caso raro), adiciona direto
    if request.user.is_authenticated:
        adicionar_ao_carrinho(
            request.user,
            camiseta,
            tamanho=tamanho,
            quantidade=quantidade,
        )
        messages.success(request, "Item adicionado ao carrinho.")
        return redirect("loja:landing")

    guardar_carrinho_pendente(
        request,
        slug=slug,
        tamanho=tamanho,
        quantidade=quantidade,
    )
    login_url = (
        f"{reverse('loja:login')}?next={quote(reverse('loja:landing'))}"
    )
    return redirect(login_url)


@htmx_login_required
@require_POST
def adicionar_carrinho(request: HttpRequest, slug: str) -> HttpResponse:
    """
    Adiciona camiseta ao carrinho via HTMX (sem reload).
    Retorna o partial do badge (#cart-badge).
    """
    camiseta = get_object_or_404(Camiseta, slug=slug, ativo=True)
    form = AdicionarCarrinhoForm(request.POST, camiseta=camiseta)
    if not form.is_valid():
        # Fallback: tamanho padrao da categoria, qtd 1
        tamanho = camiseta.tamanho_padrao
        quantidade = 1
    else:
        tamanho = form.cleaned_data["tamanho"]
        quantidade = form.cleaned_data["quantidade"]

    adicionar_ao_carrinho(
        request.user,
        camiseta,
        tamanho=tamanho,
        quantidade=quantidade,
    )

    # Toast opcional via header HX-Trigger
    response = render(
        request,
        "loja/partials/cart_badge.html",
        {"carrinho_total_itens": request.user.carrinho.total_itens},
    )
    response["HX-Trigger"] = '{"carrinhoAtualizado": true, "toast": "Adicionado ao carrinho"}'
    return response


@login_required
@require_GET
def ver_carrinho(request: HttpRequest) -> HttpResponse:
    carrinho = obter_ou_criar_carrinho(request.user)
    itens = carrinho.itens.select_related("camiseta")
    return render(
        request,
        "loja/carrinho.html",
        {"carrinho": carrinho, "itens": itens},
    )


@login_required
@require_POST
def atualizar_item_carrinho(request: HttpRequest, item_id: int) -> HttpResponse:
    """Altera quantidade (HTMX) ou remove se quantidade=0."""
    try:
        quantidade = int(request.POST.get("quantidade", 1))
    except (TypeError, ValueError):
        quantidade = 1

    atualizar_quantidade_item(request.user, item_id, quantidade)
    carrinho = obter_ou_criar_carrinho(request.user)
    itens = carrinho.itens.select_related("camiseta")

    # Se a requisição veio do HTMX, devolve só o corpo da tabela
    if getattr(request, "htmx", False):
        return render(
            request,
            "loja/partials/carrinho_tabela.html",
            {"carrinho": carrinho, "itens": itens},
        )
    return redirect("loja:carrinho")


@login_required
@require_POST
def remover_item_carrinho(request: HttpRequest, item_id: int) -> HttpResponse:
    atualizar_quantidade_item(request.user, item_id, 0)
    if getattr(request, "htmx", False):
        carrinho = obter_ou_criar_carrinho(request.user)
        itens = carrinho.itens.select_related("camiseta")
        return render(
            request,
            "loja/partials/carrinho_tabela.html",
            {"carrinho": carrinho, "itens": itens},
        )
    return redirect("loja:carrinho")


@login_required
@require_POST
def checkout(request: HttpRequest) -> HttpResponse:
    """Finaliza compra → Pedido com status Em espera / Nao entregue (+ observacao opcional)."""
    observacoes = (request.POST.get("observacoes") or "").strip()
    try:
        pedido = finalizar_compra(request.user, observacoes=observacoes)
    except ValueError as exc:
        messages.error(request, str(exc))
        return redirect("loja:carrinho")

    messages.success(
        request,
        f"Pedido {pedido.codigo_curto} gerado! Status: Em espera.",
    )
    return redirect("loja:meus_pedidos")


# ---------------------------------------------------------------------------
# Área do cliente — Meus Pedidos
# ---------------------------------------------------------------------------


@login_required
@require_GET
def meus_pedidos(request: HttpRequest) -> HttpResponse:
    pedidos = (
        Pedido.objects.filter(cliente=request.user)
        .prefetch_related("itens")
        .order_by("-criado_em")
    )
    return render(request, "loja/meus_pedidos.html", {"pedidos": pedidos})


# ---------------------------------------------------------------------------
# Área do administrador — Dashboards (bloqueio rigoroso)
# ---------------------------------------------------------------------------


@administrador_required
@require_GET
def painel_visao_geral(request: HttpRequest) -> HttpResponse:
    """Quantidade total pedida de cada tipo, com detalhe por tamanho."""
    itens = ItemPedido.objects.exclude(
        pedido__status_pagamento=Pedido.StatusPagamento.CANCELADO
    )

    # Totais por modelo (camiseta)
    por_tipo = (
        itens.values("camiseta_id", "nome_camiseta")
        .annotate(total_pedida=Sum("quantidade"))
        .order_by("-total_pedida")
    )
    # Quebra por tamanho dentro de cada modelo
    por_tamanho = (
        itens.values("camiseta_id", "tamanho")
        .annotate(qtd=Sum("quantidade"))
        .order_by("camiseta_id")
    )

    # Ordem estavel dos tamanhos (adulto + infantil)
    ordem_tamanho = {
        value: idx for idx, (value, _label) in enumerate(Camiseta.TAMANHOS_TODOS)
    }
    labels_tamanho = dict(Camiseta.TAMANHOS_TODOS)

    tamanhos_por_camiseta: dict[int, list[dict]] = {}
    for row in por_tamanho:
        cid = row["camiseta_id"]
        tamanho = row["tamanho"]
        tamanhos_por_camiseta.setdefault(cid, []).append(
            {
                "tamanho": tamanho,
                "label": labels_tamanho.get(tamanho, tamanho),
                "qtd": row["qtd"] or 0,
            }
        )
    for lista in tamanhos_por_camiseta.values():
        lista.sort(key=lambda t: ordem_tamanho.get(t["tamanho"], 999))

    # Garante que camisetas sem pedido aparecam com 0 (adulto e infantil)
    camisetas = Camiseta.objects.filter(ativo=True).order_by(
        "categoria", "-destaque", "nome"
    )
    mapa = {row["camiseta_id"]: row["total_pedida"] for row in por_tipo}
    resumo = [
        {
            "nome": c.nome,
            "slug": c.slug,
            "total_pedida": mapa.get(c.id, 0),
            "por_tamanho": tamanhos_por_camiseta.get(c.id, []),
        }
        for c in camisetas
    ]
    return render(request, "loja/admin/visao_geral.html", {"resumo": resumo})


@administrador_required
@require_GET
def painel_por_cliente(request: HttpRequest) -> HttpResponse:
    """Lista de clientes com valor total gasto (soma pedidos nao cancelados)."""
    clientes = (
        Usuario.objects.annotate(
            total_gasto=Sum(
                "pedidos__valor_total",
                filter=~Q(
                    pedidos__status_pagamento=Pedido.StatusPagamento.CANCELADO
                ),
            ),
            qtd_pedidos=Count(
                "pedidos",
                filter=~Q(
                    pedidos__status_pagamento=Pedido.StatusPagamento.CANCELADO
                ),
            ),
        )
        .order_by("-total_gasto")
    )
    return render(request, "loja/admin/por_cliente.html", {"clientes": clientes})


@administrador_required
@require_GET
def painel_por_igreja(request: HttpRequest) -> HttpResponse:
    """Quantidade de camisetas pedidas por igreja (ignora pedidos cancelados)."""
    itens = ItemPedido.objects.exclude(
        pedido__status_pagamento=Pedido.StatusPagamento.CANCELADO
    )

    # Total de camisetas por igreja
    totais = itens.values("pedido__cliente__igreja").annotate(
        total_camisetas=Sum("quantidade")
    )
    # Detalhe: modelo + tamanho dentro de cada igreja
    detalhes = (
        itens.values("pedido__cliente__igreja", "nome_camiseta", "tamanho")
        .annotate(qtd=Sum("quantidade"))
        .order_by("nome_camiseta", "tamanho")
    )
    # Pedidos e valor arrecadado por igreja (agregado direto em Pedido
    # para nao duplicar valor_total em pedidos com varios itens)
    pedidos_agg = (
        Pedido.objects.exclude(status_pagamento=Pedido.StatusPagamento.CANCELADO)
        .values("cliente__igreja")
        .annotate(qtd_pedidos=Count("id"), valor_total=Sum("valor_total"))
    )

    labels_tamanho = dict(Camiseta.TAMANHOS_TODOS)
    ordem_tamanho = {
        value: idx for idx, (value, _label) in enumerate(Camiseta.TAMANHOS_TODOS)
    }

    def _nome(valor: str | None) -> str:
        # Usuarios antigos podem nao ter igreja preenchida
        return valor or "Nao informada"

    mapa: dict[str, dict] = {}
    for row in totais:
        igreja = _nome(row["pedido__cliente__igreja"])
        mapa[igreja] = {
            "igreja": igreja,
            "total_camisetas": row["total_camisetas"] or 0,
            "modelos": [],
            "qtd_pedidos": 0,
            "valor_total": None,
        }
    for row in pedidos_agg:
        igreja = _nome(row["cliente__igreja"])
        dados = mapa.setdefault(
            igreja,
            {
                "igreja": igreja,
                "total_camisetas": 0,
                "modelos": [],
                "qtd_pedidos": 0,
                "valor_total": None,
            },
        )
        dados["qtd_pedidos"] = row["qtd_pedidos"]
        dados["valor_total"] = row["valor_total"]
    for row in detalhes:
        igreja = _nome(row["pedido__cliente__igreja"])
        if igreja in mapa:
            label = labels_tamanho.get(row["tamanho"], row["tamanho"])
            mapa[igreja]["modelos"].append(
                {
                    "texto": f"{row['qtd']}x {row['nome_camiseta']} ({label})",
                    "nome": row["nome_camiseta"],
                    "tamanho": row["tamanho"],
                    "qtd": row["qtd"],
                }
            )

    # Ordena linhas de modelo por nome e depois pela ordem de tamanho
    for dados in mapa.values():
        dados["modelos"].sort(
            key=lambda m: (m["nome"], ordem_tamanho.get(m["tamanho"], 999))
        )
        # Template usa so o texto formatado
        dados["modelos"] = [m["texto"] for m in dados["modelos"]]

    # Igrejas com mais camisetas primeiro
    igrejas = sorted(
        mapa.values(), key=lambda d: d["total_camisetas"], reverse=True
    )
    return render(request, "loja/admin/por_igreja.html", {"igrejas": igrejas})


@administrador_required
@require_GET
def painel_pedidos(request: HttpRequest) -> HttpResponse:
    """Tabela de cada compra individual + gestão inline de status."""
    pedidos = Pedido.objects.select_related("cliente").prefetch_related("itens")
    return render(
        request,
        "loja/admin/pedidos.html",
        {
            "pedidos": pedidos,
            "status_pagamento_choices": Pedido.StatusPagamento.choices,
            "status_entrega_choices": Pedido.StatusEntrega.choices,
        },
    )


@administrador_required
@require_POST
def atualizar_status_pedido(request: HttpRequest, pedido_id: int) -> HttpResponse:
    """
    Atualiza status de pagamento e/ou entrega via HTMX (inline, sem refresh).
    Apenas administradores — clientes → 403 via decorator.
    """
    pedido = get_object_or_404(Pedido, pk=pedido_id)
    form = AtualizarStatusForm(request.POST)
    if not form.is_valid():
        return HttpResponseForbidden("Dados inválidos.")

    pagamento = form.cleaned_data.get("status_pagamento")
    entrega = form.cleaned_data.get("status_entrega")
    campos = []
    if pagamento:
        pedido.status_pagamento = pagamento
        campos.append("status_pagamento")
    if entrega:
        pedido.status_entrega = entrega
        campos.append("status_entrega")
    if campos:
        campos.append("atualizado_em")
        pedido.save(update_fields=campos)

    # Resposta parcial: só a linha da tabela
    if getattr(request, "htmx", False):
        return render(
            request,
            "loja/partials/pedido_row.html",
            {
                "pedido": pedido,
                "status_pagamento_choices": Pedido.StatusPagamento.choices,
                "status_entrega_choices": Pedido.StatusEntrega.choices,
            },
        )
    return redirect("loja:painel_pedidos")


@administrador_required
@require_GET
def exportar_excel(request: HttpRequest) -> HttpResponse:
    """
    Baixa planilha Excel com producao, visao geral, igrejas, clientes e pedidos.
    Pedidos cancelados nao entram nas abas de producao/resumo.
    """
    buffer = gerar_excel_painel()
    response = HttpResponse(
        buffer.getvalue(),
        content_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
    )
    response["Content-Disposition"] = (
        f'attachment; filename="{nome_arquivo_excel()}"'
    )
    return response
