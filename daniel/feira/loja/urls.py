"""Rotas da loja - painel /painel/* estritamente para administradores."""

from django.urls import path

from . import views

app_name = "loja"

urlpatterns = [
    path("", views.landing, name="landing"),
    path("cadastrar/", views.cadastro, name="cadastro"),
    path("entrar/", views.entrar, name="login"),
    path("sair/", views.sair, name="logout"),
    path(
        "carrinho/adicionar/<slug:slug>/",
        views.adicionar_carrinho,
        name="adicionar_carrinho",
    ),
    # Visitante: guarda intencao na sessao e redireciona ao login
    path(
        "carrinho/intencao/<slug:slug>/",
        views.intencao_carrinho,
        name="intencao_carrinho",
    ),
    path("carrinho/", views.ver_carrinho, name="carrinho"),
    path(
        "carrinho/item/<int:item_id>/",
        views.atualizar_item_carrinho,
        name="atualizar_item",
    ),
    path(
        "carrinho/item/<int:item_id>/remover/",
        views.remover_item_carrinho,
        name="remover_item",
    ),
    path("carrinho/finalizar/", views.checkout, name="checkout"),
    path("meus-pedidos/", views.meus_pedidos, name="meus_pedidos"),
    path("painel/", views.painel_visao_geral, name="painel_visao_geral"),
    path("painel/clientes/", views.painel_por_cliente, name="painel_por_cliente"),
    path("painel/igrejas/", views.painel_por_igreja, name="painel_por_igreja"),
    path("painel/pedidos/", views.painel_pedidos, name="painel_pedidos"),
    path(
        "painel/pedidos/<int:pedido_id>/status/",
        views.atualizar_status_pedido,
        name="atualizar_status_pedido",
    ),
]
