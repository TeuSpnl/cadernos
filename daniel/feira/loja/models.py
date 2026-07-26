"""
Modelagem da loja de camisetas da Feira Missionária.

- Usuario: cadastro simplificado (nome completo + senha); staff = admin.
- Camiseta: os 3 tipos exclusivos (R$ 50,00).
- Carrinho / ItemCarrinho: persistidos no banco para usuários logados.
- Pedido / ItemPedido: snapshot da compra + status paralelo de pagamento/entrega.
"""

from __future__ import annotations

import uuid
from decimal import Decimal

from django.contrib.auth.models import AbstractBaseUser, BaseUserManager, PermissionsMixin
from django.core.validators import MinValueValidator
from django.db import models
from django.db.models import Sum
from django.utils.text import slugify


class UsuarioManager(BaseUserManager):
    """Manager: login por username; nome_completo e so identificacao."""

    def create_user(
        self,
        username: str,
        nome_completo: str = "",
        password: str | None = None,
        **extra,
    ):
        if not username or not username.strip():
            raise ValueError("O nome de usuario e obrigatorio.")
        username = username.strip().lower()
        nome = (nome_completo or username).strip()
        user = self.model(username=username, nome_completo=nome, **extra)
        user.set_password(password)
        user.save(using=self._db)
        return user

    def create_superuser(
        self,
        username: str,
        nome_completo: str = "",
        password: str | None = None,
        **extra,
    ):
        extra.setdefault("is_staff", True)
        extra.setdefault("is_superuser", True)
        if extra.get("is_staff") is not True:
            raise ValueError("Superuser precisa de is_staff=True.")
        if extra.get("is_superuser") is not True:
            raise ValueError("Superuser precisa de is_superuser=True.")
        return self.create_user(username, nome_completo, password, **extra)


class Usuario(AbstractBaseUser, PermissionsMixin):
    """
    Cliente (e admin, se is_staff).
    Login = username (curto). Nome completo fica so para exibicao/pedidos.
    """

    username = models.CharField(
        "Usuario",
        max_length=50,
        unique=True,
        help_text="Login curto (ex.: joao.silva). Sem espacos.",
    )
    nome_completo = models.CharField(
        "Nome completo",
        max_length=255,
        unique=True,
        help_text="Nao pode haver dois cadastros com o mesmo nome.",
    )
    telefone = models.CharField(
        "Telefone para contato",
        max_length=20,
        blank=True,
        help_text="WhatsApp/celular com DDD.",
    )
    igreja = models.CharField(
        "Igreja/Congregacao",
        max_length=255,
        blank=True,
    )
    is_staff = models.BooleanField(
        "Acesso administrativo",
        default=False,
        help_text="Libera o painel gerencial (dashboards e alteracao de status).",
    )
    is_active = models.BooleanField(default=True)
    date_joined = models.DateTimeField(auto_now_add=True)

    objects = UsuarioManager()

    USERNAME_FIELD = "username"
    REQUIRED_FIELDS = ["nome_completo"]

    class Meta:
        verbose_name = "usuario"
        verbose_name_plural = "usuarios"
        ordering = ["nome_completo"]

    def __str__(self) -> str:
        return f"{self.nome_completo} (@{self.username})"

    def save(self, *args, **kwargs):
        # Normaliza username em minusculas
        if self.username:
            self.username = self.username.strip().lower()
        super().save(*args, **kwargs)

    @property
    def is_administrador(self) -> bool:
        """Atalho: staff ou superuser."""
        return bool(self.is_staff or self.is_superuser)


class Camiseta(models.Model):
    """Tipo de camiseta a venda (modelo + categoria adulto/infantil)."""

    class Modelo(models.TextChoices):
        VERDE_MILITAR = "verde-militar", "Verde Militar"
        BRANCA_CLASSIC = "branca-classic", "Branca Classic"
        BEGE_AREIA = "bege-areia", "Bege Areia Elegance"

    class Categoria(models.TextChoices):
        ADULTO = "adulto", "Adulto"
        INFANTIL = "infantil", "Infantil"

    # Tamanhos por categoria (value curto no banco, label amigavel na UI)
    TAMANHOS_ADULTO = [
        ("PP", "PP"),
        ("P", "P"),
        ("M", "M"),
        ("G", "G"),
        ("GG", "GG"),
        ("XGG", "XGG"),
        ("P-BL", "P - BabyLook"),
        ("M-BL", "M - BabyLook"),
        ("G-BL", "G - BabyLook"),
        ("GG-BL", "GG - BabyLook"),
    ]
    TAMANHOS_INFANTIL = [
        ("1", "1"),
        ("2", "2"),
        ("4", "4"),
        ("6", "6"),
        ("8", "8"),
        ("10", "10"),
    ]
    TAMANHOS_TODOS = TAMANHOS_ADULTO + TAMANHOS_INFANTIL

    slug = models.SlugField(max_length=64, unique=True)
    nome = models.CharField(max_length=120)
    descricao = models.TextField(blank=True)
    categoria = models.CharField(
        max_length=20,
        choices=Categoria.choices,
        default=Categoria.ADULTO,
        help_text="Adulto (R$ 50) ou Infantil (R$ 40) — produtos separados.",
    )
    preco = models.DecimalField(
        max_digits=8,
        decimal_places=2,
        default=Decimal("50.00"),
        validators=[MinValueValidator(Decimal("0.01"))],
    )
    imagem = models.CharField(
        max_length=255,
        help_text="Caminho relativo sob /images/ (ex.: verde-militar.jpeg)",
    )
    destaque = models.BooleanField(
        default=False,
        help_text="Marca a peca principal (centro no desktop / topo no mobile).",
    )
    ativo = models.BooleanField(default=True)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "camiseta"
        verbose_name_plural = "camisetas"
        ordering = ["categoria", "-destaque", "nome"]

    def __str__(self) -> str:
        return self.nome

    def save(self, *args, **kwargs):
        # Garante slug a partir do nome se vier vazio
        if not self.slug:
            self.slug = slugify(self.nome)
        super().save(*args, **kwargs)

    @property
    def is_infantil(self) -> bool:
        return self.categoria == self.Categoria.INFANTIL

    @property
    def tamanhos_disponiveis(self) -> list[tuple[str, str]]:
        """Opcoes de tamanho conforme a categoria do produto."""
        if self.is_infantil:
            return list(self.TAMANHOS_INFANTIL)
        return list(self.TAMANHOS_ADULTO)

    @property
    def tamanho_padrao(self) -> str:
        return "6" if self.is_infantil else "M"


class Carrinho(models.Model):
    """Um carrinho por usuario logado — estado persistido no banco."""

    usuario = models.OneToOneField(
        Usuario,
        on_delete=models.CASCADE,
        related_name="carrinho",
    )
    atualizado_em = models.DateTimeField(auto_now=True)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "carrinho"
        verbose_name_plural = "carrinhos"

    def __str__(self) -> str:
        return f"Carrinho de {self.usuario}"

    @property
    def total_itens(self) -> int:
        return self.itens.aggregate(t=Sum("quantidade"))["t"] or 0

    @property
    def valor_total(self) -> Decimal:
        total = Decimal("0.00")
        for item in self.itens.select_related("camiseta"):
            total += item.subtotal
        return total


class ItemCarrinho(models.Model):
    """Linha do carrinho: camiseta + tamanho + quantidade."""

    # Constantes usadas como fallback no codigo (services/views)
    class Tamanho:
        M = "M"
        INFANTIL_PADRAO = "6"

    carrinho = models.ForeignKey(
        Carrinho,
        on_delete=models.CASCADE,
        related_name="itens",
    )
    camiseta = models.ForeignKey(
        Camiseta,
        on_delete=models.CASCADE,
        related_name="itens_carrinho",
    )
    tamanho = models.CharField(
        max_length=8,
        choices=Camiseta.TAMANHOS_TODOS,
        default="M",
    )
    quantidade = models.PositiveIntegerField(
        default=1,
        validators=[MinValueValidator(1)],
    )

    class Meta:
        verbose_name = "item do carrinho"
        verbose_name_plural = "itens do carrinho"
        # Mesma camiseta + mesmo tamanho = uma linha
        unique_together = ("carrinho", "camiseta", "tamanho")

    def __str__(self) -> str:
        return f"{self.quantidade}x {self.camiseta} ({self.get_tamanho_display()})"

    @property
    def subtotal(self) -> Decimal:
        return self.camiseta.preco * self.quantidade


class Pedido(models.Model):
    """Pedido gerado na finalizacao da compra."""

    class StatusPagamento(models.TextChoices):
        EM_ESPERA = "em_espera", "Em espera"
        PAGO = "pago", "Pago"
        CANCELADO = "cancelado", "Cancelado"

    class StatusEntrega(models.TextChoices):
        NAO_ENTREGUE = "nao_entregue", "Nao entregue"
        ENTREGUE = "entregue", "Entregue"

    # ID unico legivel (alem do PK numerico)
    codigo = models.UUIDField(default=uuid.uuid4, unique=True, editable=False)
    cliente = models.ForeignKey(
        Usuario,
        on_delete=models.PROTECT,
        related_name="pedidos",
    )
    valor_total = models.DecimalField(max_digits=10, decimal_places=2)
    status_pagamento = models.CharField(
        max_length=20,
        choices=StatusPagamento.choices,
        default=StatusPagamento.EM_ESPERA,
    )
    status_entrega = models.CharField(
        max_length=20,
        choices=StatusEntrega.choices,
        default=StatusEntrega.NAO_ENTREGUE,
    )
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)
    observacoes = models.TextField(blank=True)

    class Meta:
        verbose_name = "pedido"
        verbose_name_plural = "pedidos"
        ordering = ["-criado_em"]

    def __str__(self) -> str:
        return f"Pedido {self.codigo_curto} — {self.cliente}"

    @property
    def codigo_curto(self) -> str:
        """UUID abreviado para exibicao (ex.: A1B2C3D4)."""
        return str(self.codigo).split("-")[0].upper()


class ItemPedido(models.Model):
    """Snapshot do item no momento da compra (preco congelado)."""

    pedido = models.ForeignKey(
        Pedido,
        on_delete=models.CASCADE,
        related_name="itens",
    )
    camiseta = models.ForeignKey(
        Camiseta,
        on_delete=models.PROTECT,
        related_name="itens_pedido",
    )
    nome_camiseta = models.CharField(max_length=120)
    # max_length maior: BabyLook (ex.: GG-BL) e legado
    tamanho = models.CharField(max_length=8)
    quantidade = models.PositiveIntegerField()
    preco_unitario = models.DecimalField(max_digits=8, decimal_places=2)

    class Meta:
        verbose_name = "item do pedido"
        verbose_name_plural = "itens do pedido"

    def __str__(self) -> str:
        return f"{self.quantidade}x {self.nome_camiseta} ({self.tamanho})"

    @property
    def subtotal(self) -> Decimal:
        return self.preco_unitario * self.quantidade

    def get_tamanho_display(self) -> str:
        return dict(Camiseta.TAMANHOS_TODOS).get(self.tamanho, self.tamanho)