# Generated manually: adiciona username para login (separado do nome completo)

import re

from django.db import migrations, models
from django.utils.text import slugify


def popular_usernames(apps, schema_editor):
    """Gera username a partir do nome_completo para usuarios ja existentes."""
    Usuario = apps.get_model("loja", "Usuario")
    usados = set()
    for user in Usuario.objects.all():
        base = slugify(user.nome_completo).replace("-", ".")[:40] or "user"
        base = re.sub(r"[^a-z0-9._-]", "", base.lower()) or "user"
        candidato = base
        i = 1
        while candidato in usados or Usuario.objects.filter(username=candidato).exclude(pk=user.pk).exists():
            i += 1
            candidato = f"{base}{i}"
        user.username = candidato
        user.save(update_fields=["username"])
        usados.add(candidato)


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("loja", "0001_initial"),
    ]

    operations = [
        # 1) username temporariamente opcional
        migrations.AddField(
            model_name="usuario",
            name="username",
            field=models.CharField(
                blank=True,
                default="",
                help_text="Login curto (ex.: joao.silva). Sem espacos.",
                max_length=50,
                verbose_name="Usuario",
            ),
        ),
        # 2) preenche username dos usuarios existentes
        migrations.RunPython(popular_usernames, noop_reverse),
        # 3) username unico e obrigatorio
        migrations.AlterField(
            model_name="usuario",
            name="username",
            field=models.CharField(
                help_text="Login curto (ex.: joao.silva). Sem espacos.",
                max_length=50,
                unique=True,
                verbose_name="Usuario",
            ),
        ),
        # 4) nome_completo deixa de ser unico (varios podem ter nomes parecidos)
        migrations.AlterField(
            model_name="usuario",
            name="nome_completo",
            field=models.CharField(max_length=255, verbose_name="Nome completo"),
        ),
        migrations.AlterModelOptions(
            name="usuario",
            options={
                "ordering": ["nome_completo"],
                "verbose_name": "usuario",
                "verbose_name_plural": "usuarios",
            },
        ),
    ]
