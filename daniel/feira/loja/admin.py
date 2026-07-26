"""Admin Django nativo - complemento ao painel HTMX."""

from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin

from .models import Camiseta, Carrinho, ItemCarrinho, ItemPedido, Pedido, Usuario


@admin.register(Usuario)
class UsuarioAdmin(BaseUserAdmin):
    """Admin: login por username; nome_completo para exibicao."""

    list_display = (
        "username",
        "nome_completo",
        "telefone",
        "igreja",
        "is_staff",
        "is_superuser",
        "is_active",
        "date_joined",
    )
    list_filter = ("is_staff", "is_superuser", "is_active", "igreja")
    search_fields = ("username", "nome_completo", "telefone", "igreja")
    ordering = ("username",)
    fieldsets = (
        (None, {"fields": ("username", "nome_completo", "password")}),
        ("Contato", {"fields": ("telefone", "igreja")}),
        (
            "Permissoes",
            {"fields": ("is_active", "is_staff", "is_superuser", "groups", "user_permissions")},
        ),
    )
    add_fieldsets = (
        (
            None,
            {
                "classes": ("wide",),
                "fields": (
                    "username",
                    "nome_completo",
                    "password1",
                    "password2",
                    "is_staff",
                    "is_superuser",
                ),
            },
        ),
    )
    filter_horizontal = ("groups", "user_permissions")


@admin.register(Camiseta)
class CamisetaAdmin(admin.ModelAdmin):
    list_display = ("nome", "slug", "categoria", "preco", "destaque", "ativo")
    prepopulated_fields = {"slug": ("nome",)}
    list_filter = ("categoria", "ativo", "destaque")
    search_fields = ("nome", "slug")

class ItemCarrinhoInline(admin.TabularInline):
    model = ItemCarrinho
    extra = 0


@admin.register(Carrinho)
class CarrinhoAdmin(admin.ModelAdmin):
    list_display = ("usuario", "atualizado_em")
    inlines = [ItemCarrinhoInline]


class ItemPedidoInline(admin.TabularInline):
    model = ItemPedido
    extra = 0
    readonly_fields = (
        "camiseta",
        "nome_camiseta",
        "tamanho",
        "quantidade",
        "preco_unitario",
    )


@admin.register(Pedido)
class PedidoAdmin(admin.ModelAdmin):
    list_display = (
        "codigo_curto",
        "cliente",
        "valor_total",
        "status_pagamento",
        "status_entrega",
        "criado_em",
    )
    list_filter = ("status_pagamento", "status_entrega")
    search_fields = ("codigo", "cliente__nome_completo", "cliente__username")
    inlines = [ItemPedidoInline]
    readonly_fields = ("codigo", "valor_total", "criado_em", "atualizado_em")
