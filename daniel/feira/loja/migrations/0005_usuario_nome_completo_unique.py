# Nome completo volta a ser unico (nao pode haver dois cadastros iguais)

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("loja", "0004_usuario_contato_igreja"),
    ]

    operations = [
        migrations.AlterField(
            model_name="usuario",
            name="nome_completo",
            field=models.CharField(
                help_text="Nao pode haver dois cadastros com o mesmo nome.",
                max_length=255,
                unique=True,
                verbose_name="Nome completo",
            ),
        ),
    ]
