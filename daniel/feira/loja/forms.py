"""Formularios: cadastro/login e acoes do carrinho."""

import re

from django import forms
from django.contrib.auth.forms import AuthenticationForm

from .models import Camiseta, ItemCarrinho, Usuario


# Opcoes de igreja (a ultima permite texto livre)
IGREJAS = [
    "Igreja Presbiteriana de Vitoria da Conquista (IPVC)",
    "Igreja Presbiteriana Urbis VI",
    "Igreja Presbiteriana Alianca",
    "Congregacao Presbiteriana do Miro Cairo",
    "Congregacao Presbiteriana do Boa Vista",
]
IGREJA_OUTRA = "__outra__"


class CadastroForm(forms.ModelForm):
    """Cadastro: usuario + nome completo + telefone + igreja + senha."""

    telefone = forms.CharField(
        label="Telefone para contato",
        max_length=20,
        widget=forms.TextInput(
            attrs={
                "placeholder": "(77) 90000-0000",
                "inputmode": "tel",
                "required": True,
            }
        ),
    )
    igreja = forms.ChoiceField(
        label="Igreja/Congregacao",
        choices=[("", "Selecione...")]
        + [(nome, nome) for nome in IGREJAS]
        + [(IGREJA_OUTRA, "Outra (escrever)")],
        widget=forms.Select(attrs={"required": True, "class": "app-select"}),
    )
    igreja_outra = forms.CharField(
        label="Qual igreja/congregacao?",
        max_length=255,
        required=False,
        # Obrigatorio no navegador so quando "Outra" esta selecionada
        widget=forms.TextInput(
            attrs={
                "placeholder": "Escreva o nome da igreja",
                "x-bind:required": "igreja === '__outra__'",
            }
        ),
    )
    password1 = forms.CharField(
        label="Senha",
        min_length=6,
        widget=forms.PasswordInput(
            attrs={
                "autocomplete": "new-password",
                "required": True,
                "minlength": "6",
            }
        ),
    )
    password2 = forms.CharField(
        label="Confirmar senha",
        widget=forms.PasswordInput(
            attrs={"autocomplete": "new-password", "required": True}
        ),
    )

    class Meta:
        model = Usuario
        fields = ("username", "nome_completo")
        labels = {
            "username": "Usuario (login)",
            "nome_completo": "Nome completo",
        }
        help_texts = {
            "username": "Use letras, numeros, ponto ou underline. Ex.: maria.souza",
        }
        widgets = {
            "username": forms.TextInput(attrs={"required": True, "autocomplete": "username"}),
            "nome_completo": forms.TextInput(
                attrs={"required": True, "autocomplete": "name"}
            ),
        }

    def clean_username(self):
        username = self.cleaned_data["username"].strip().lower()
        if not re.fullmatch(r"[a-z0-9._-]{3,50}", username):
            raise forms.ValidationError(
                "Usuario invalido. Use 3-50 caracteres: a-z, 0-9, . _ -"
            )
        if Usuario.objects.filter(username=username).exists():
            raise forms.ValidationError("Este usuario ja esta em uso.")
        return username

    def clean_nome_completo(self):
        nome = self.cleaned_data["nome_completo"].strip()
        # Precisa de pelo menos duas palavras com 2+ letras cada
        partes = [p for p in re.split(r"\s+", nome) if len(p) >= 2]
        if len(partes) < 2:
            raise forms.ValidationError("Escreva o nome completo")
        # Nao pode haver dois cadastros com o mesmo nome (ignora maiusculas)
        if Usuario.objects.filter(nome_completo__iexact=nome).exists():
            raise forms.ValidationError(
                "Ja existe um cadastro com este nome completo."
            )
        return nome

    def clean_telefone(self):
        telefone = self.cleaned_data["telefone"].strip()
        digitos = re.sub(r"\D", "", telefone)
        if len(digitos) < 10:
            raise forms.ValidationError(
                "Informe um telefone valido com DDD."
            )
        return telefone

    def clean(self):
        cleaned = super().clean()

        # Resolve igreja: se "outra", exige o texto livre
        igreja = cleaned.get("igreja")
        outra = (cleaned.get("igreja_outra") or "").strip()
        if igreja == IGREJA_OUTRA:
            if not outra:
                self.add_error("igreja_outra", "Escreva o nome da igreja.")
            else:
                cleaned["igreja"] = outra
        elif not igreja:
            self.add_error("igreja", "Selecione a sua igreja/congregacao.")

        p1 = cleaned.get("password1")
        p2 = cleaned.get("password2")
        if p1 and p2 and p1 != p2:
            self.add_error("password2", "As senhas nao coincidem.")
        return cleaned

    def save(self, commit=True):
        user = super().save(commit=False)
        user.telefone = self.cleaned_data["telefone"]
        user.igreja = self.cleaned_data["igreja"]
        user.set_password(self.cleaned_data["password1"])
        if commit:
            user.save()
        return user


class LoginForm(AuthenticationForm):
    """Login por username (case-insensitive). Senha continua case-sensitive."""

    username = forms.CharField(label="Usuario")
    password = forms.CharField(label="Senha", widget=forms.PasswordInput)

    error_messages = {
        "invalid_login": "Usuario ou senha incorretos. Verifique e tente de novo.",
        "inactive": "Esta conta esta desativada.",
    }

    def clean_username(self):
        # Mateus == mateus == MATEUS — senha nao e alterada aqui
        return self.cleaned_data["username"].strip().lower()


class AdicionarCarrinhoForm(forms.Form):
    """Quantidade e tamanho ao adicionar ao carrinho via HTMX."""

    tamanho = forms.CharField()
    quantidade = forms.IntegerField(min_value=1, max_value=20, initial=1)

    def __init__(self, *args, camiseta=None, **kwargs):
        # camiseta define quais tamanhos sao validos (adulto x infantil)
        self.camiseta = camiseta
        super().__init__(*args, **kwargs)
        if camiseta is not None:
            opcoes = camiseta.tamanhos_disponiveis
            self.fields["tamanho"] = forms.ChoiceField(
                choices=opcoes,
                initial=camiseta.tamanho_padrao,
            )
        else:
            self.fields["tamanho"] = forms.ChoiceField(
                choices=Camiseta.TAMANHOS_TODOS,
                initial="M",
            )

    def clean_tamanho(self):
        tamanho = self.cleaned_data["tamanho"]
        if self.camiseta is not None:
            validos = {v for v, _ in self.camiseta.tamanhos_disponiveis}
            if tamanho not in validos:
                raise forms.ValidationError(
                    "Tamanho invalido para este modelo."
                )
        return tamanho


class AtualizarStatusForm(forms.Form):
    """Admin: atualizacao inline de status (pagamento ou entrega)."""

    status_pagamento = forms.ChoiceField(choices=[], required=False)
    status_entrega = forms.ChoiceField(choices=[], required=False)

    def __init__(self, *args, **kwargs):
        from .models import Pedido

        super().__init__(*args, **kwargs)
        self.fields["status_pagamento"].choices = Pedido.StatusPagamento.choices
        self.fields["status_entrega"].choices = Pedido.StatusEntrega.choices
