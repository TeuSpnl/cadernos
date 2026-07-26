"""Comando: python manage.py seed_camisetas — 3 adultas + 3 infantis."""

from decimal import Decimal

from django.core.management.base import BaseCommand

from loja.models import Camiseta


# Mesmos modelos; infantis sao produtos separados (preco R$ 40)
MODELOS = [
    {
        "base_slug": "verde-militar",
        "nome_base": "Verde Militar",
        "descricao": (
            "Robustez e estilo com a grande arvore em tom sobre tom. "
            "Logo no peito e Mateus 7:17 nas costas, em dourado/bege."
        ),
        "imagem": "verde-militar.jpeg",
        "destaque_adulto": True,
    },
    {
        "base_slug": "branca-classic",
        "nome_base": "Branca Classic",
        "descricao": (
            "Classica e versatil, com o logo no peito e lettering vibrante "
            "nas costas — Mateus 7:17 em tons de verde e dourado."
        ),
        "imagem": "branca-classic.jpeg",
        "destaque_adulto": False,
    },
    {
        "base_slug": "bege-areia",
        "nome_base": "Bege Areia Elegance",
        "descricao": (
            "Tom areia com arvore tonal pelo corpo, logo no peito e mensagem "
            "em letra cursiva nas costas."
        ),
        "imagem": "bege-areia.jpeg",
        "destaque_adulto": False,
    },
]


class Command(BaseCommand):
    help = "Cadastra (ou atualiza) as camisetas adulto e infantil da Feira."

    def handle(self, *args, **options):
        for modelo in MODELOS:
            # --- Adulto (R$ 50) ---
            adulto, created = Camiseta.objects.update_or_create(
                slug=modelo["base_slug"],
                defaults={
                    "nome": modelo["nome_base"],
                    "descricao": modelo["descricao"],
                    "imagem": modelo["imagem"],
                    "categoria": Camiseta.Categoria.ADULTO,
                    "preco": Decimal("50.00"),
                    "destaque": modelo["destaque_adulto"],
                    "ativo": True,
                },
            )
            acao = "Criada" if created else "Atualizada"
            self.stdout.write(
                self.style.SUCCESS(f"{acao}: {adulto.nome} (adulto)")
            )

            # --- Infantil (R$ 40), mesmo visual, produto separado ---
            infantil, created = Camiseta.objects.update_or_create(
                slug=f"{modelo['base_slug']}-infantil",
                defaults={
                    "nome": f"{modelo['nome_base']} Infantil",
                    "descricao": modelo["descricao"],
                    "imagem": modelo["imagem"],
                    "categoria": Camiseta.Categoria.INFANTIL,
                    "preco": Decimal("40.00"),
                    "destaque": False,
                    "ativo": True,
                },
            )
            acao = "Criada" if created else "Atualizada"
            self.stdout.write(
                self.style.SUCCESS(f"{acao}: {infantil.nome} (infantil)")
            )
