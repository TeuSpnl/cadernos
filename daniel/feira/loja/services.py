"""Servicos de dominio: carrinho e checkout (regras de negocio)."""

from __future__ import annotations

from decimal import Decimal

from django.db import transaction
from django.shortcuts import get_object_or_404

from .models import Camiseta, Carrinho, ItemCarrinho, ItemPedido, Pedido, Usuario


def obter_ou_criar_carrinho(usuario: Usuario) -> Carrinho:
    """Garante um carrinho persistido para o usuario logado."""
    carrinho, _ = Carrinho.objects.get_or_create(usuario=usuario)
    return carrinho


def adicionar_ao_carrinho(
    usuario: Usuario,
    camiseta: Camiseta,
    *,
    tamanho: str = ItemCarrinho.Tamanho.M,
    quantidade: int = 1,
) -> ItemCarrinho:
    """Soma quantidade se a linha (camiseta+tamanho) ja existir."""
    if quantidade < 1:
        raise ValueError("Quantidade deve ser pelo menos 1.")
    if not camiseta.ativo:
        raise ValueError("Esta camiseta nao esta disponivel.")

    # Garante que o tamanho pertence a categoria do produto
    validos = {v for v, _ in camiseta.tamanhos_disponiveis}
    if tamanho not in validos:
        tamanho = camiseta.tamanho_padrao

    carrinho = obter_ou_criar_carrinho(usuario)
    item, criado = ItemCarrinho.objects.get_or_create(
        carrinho=carrinho,
        camiseta=camiseta,
        tamanho=tamanho,
        defaults={"quantidade": quantidade},
    )
    if not criado:
        item.quantidade += quantidade
        item.save(update_fields=["quantidade"])
    carrinho.save(update_fields=["atualizado_em"])
    return item


def atualizar_quantidade_item(
    usuario: Usuario,
    item_id: int,
    quantidade: int,
) -> ItemCarrinho | None:
    """Atualiza quantidade; quantidade <= 0 remove o item."""
    carrinho = obter_ou_criar_carrinho(usuario)
    item = get_object_or_404(ItemCarrinho, pk=item_id, carrinho=carrinho)
    if quantidade <= 0:
        item.delete()
        return None
    item.quantidade = quantidade
    item.save(update_fields=["quantidade"])
    return item


# Chave de sessao: item escolhido antes do login/cadastro
CARRINHO_PENDENTE_KEY = "carrinho_pendente"


def guardar_carrinho_pendente(
    request,
    *,
    slug: str,
    tamanho: str,
    quantidade: int,
) -> None:
    """Guarda na sessao o item que o visitante quis adicionar antes de autenticar."""
    request.session[CARRINHO_PENDENTE_KEY] = {
        "slug": slug,
        "tamanho": tamanho,
        "quantidade": int(quantidade),
    }
    request.session.modified = True


def aplicar_carrinho_pendente(request) -> bool:
    """
    Se houver item pendente na sessao e o usuario estiver logado,
    adiciona ao carrinho e limpa a sessao. Retorna True se aplicou.
    """
    if not getattr(request.user, "is_authenticated", False):
        return False

    pendente = request.session.pop(CARRINHO_PENDENTE_KEY, None)
    if not pendente:
        return False

    slug = pendente.get("slug")
    try:
        quantidade = max(1, int(pendente.get("quantidade") or 1))
    except (TypeError, ValueError):
        quantidade = 1

    camiseta = Camiseta.objects.filter(slug=slug, ativo=True).first()
    if not camiseta:
        return False

    # Se o tamanho pendente nao bater com a categoria, usa o padrao
    tamanho = pendente.get("tamanho") or camiseta.tamanho_padrao
    validos = {v for v, _ in camiseta.tamanhos_disponiveis}
    if tamanho not in validos:
        tamanho = camiseta.tamanho_padrao

    adicionar_ao_carrinho(
        request.user,
        camiseta,
        tamanho=tamanho,
        quantidade=quantidade,
    )
    return True


@transaction.atomic
def finalizar_compra(usuario: Usuario, observacoes: str = "") -> Pedido:
    """
    Converte o carrinho em Pedido + ItemPedido e esvazia o carrinho.
    Status inicial: pagamento Em espera / entrega Nao entregue.
    """
    carrinho = obter_ou_criar_carrinho(usuario)
    itens = list(carrinho.itens.select_related("camiseta"))
    if not itens:
        raise ValueError("Seu carrinho esta vazio.")

    valor_total = Decimal("0.00")
    for item in itens:
        valor_total += item.subtotal

    pedido = Pedido.objects.create(
        cliente=usuario,
        valor_total=valor_total,
        status_pagamento=Pedido.StatusPagamento.EM_ESPERA,
        status_entrega=Pedido.StatusEntrega.NAO_ENTREGUE,
        observacoes=(observacoes or "").strip(),
    )

    ItemPedido.objects.bulk_create(
        [
            ItemPedido(
                pedido=pedido,
                camiseta=item.camiseta,
                nome_camiseta=item.camiseta.nome,
                tamanho=item.tamanho,
                quantidade=item.quantidade,
                preco_unitario=item.camiseta.preco,
            )
            for item in itens
        ]
    )

    carrinho.itens.all().delete()
    return pedido
