# Categoria adulto/infantil + novos tamanhos (adulto BabyLook, infantil 1-10)

from django.db import migrations, models


def remapear_tamanhos(apps, schema_editor):
    """Converte tamanhos legados do carrinho e dos pedidos."""
    ItemCarrinho = apps.get_model("loja", "ItemCarrinho")
    ItemPedido = apps.get_model("loja", "ItemPedido")

    # XG/XXG -> GG/XGG; INF (antigo generico) -> M em itens do carrinho/pedido
    mapa = {"XG": "GG", "XXG": "XGG", "INF": "M"}
    for Model in (ItemCarrinho, ItemPedido):
        for antigo, novo in mapa.items():
            Model.objects.filter(tamanho=antigo).update(tamanho=novo)


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("loja", "0005_usuario_nome_completo_unique"),
    ]

    operations = [
        migrations.AddField(
            model_name="camiseta",
            name="categoria",
            field=models.CharField(
                choices=[("adulto", "Adulto"), ("infantil", "Infantil")],
                default="adulto",
                help_text="Adulto (R$ 50) ou Infantil (R$ 40) - produtos separados.",
                max_length=20,
            ),
        ),
        migrations.AlterField(
            model_name="itemcarrinho",
            name="tamanho",
            field=models.CharField(
                choices=[
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
                    ("1", "1"),
                    ("2", "2"),
                    ("4", "4"),
                    ("6", "6"),
                    ("8", "8"),
                    ("10", "10"),
                ],
                default="M",
                max_length=8,
            ),
        ),
        migrations.AlterField(
            model_name="itempedido",
            name="tamanho",
            field=models.CharField(max_length=8),
        ),
        migrations.AlterModelOptions(
            name="camiseta",
            options={
                "ordering": ["categoria", "-destaque", "nome"],
                "verbose_name": "camiseta",
                "verbose_name_plural": "camisetas",
            },
        ),
        migrations.RunPython(remapear_tamanhos, noop_reverse),
    ]
