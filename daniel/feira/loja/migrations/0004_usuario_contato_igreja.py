# Adiciona telefone e igreja ao cadastro do cliente

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("loja", "0003_alter_usuario_is_staff"),
    ]

    operations = [
        migrations.AddField(
            model_name="usuario",
            name="telefone",
            field=models.CharField(
                blank=True,
                default="",
                help_text="WhatsApp/celular com DDD.",
                max_length=20,
                verbose_name="Telefone para contato",
            ),
        ),
        migrations.AddField(
            model_name="usuario",
            name="igreja",
            field=models.CharField(
                blank=True,
                default="",
                max_length=255,
                verbose_name="Igreja/Congregacao",
            ),
        ),
    ]
