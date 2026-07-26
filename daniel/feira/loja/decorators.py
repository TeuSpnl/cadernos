"""
Decorators e mixins de seguranca.

Dashboards e alteracao de status: apenas is_staff / is_superuser.
Clientes comuns -> 403.
"""

from functools import wraps
from urllib.parse import quote

from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.http import HttpResponse
from django.shortcuts import redirect
from django.urls import reverse


def administrador_required(view_func):
    """Exige autenticacao + permissao de administrador. Clientes recebem 403."""

    @wraps(view_func)
    @login_required
    def _wrapped(request, *args, **kwargs):
        user = request.user
        if not (user.is_staff or user.is_superuser):
            raise PermissionDenied(
                "Acesso restrito a administradores da Feira Missionaria."
            )
        return view_func(request, *args, **kwargs)

    return _wrapped


def _eh_htmx(request) -> bool:
    """Detecta pedido HTMX (middleware ou header)."""
    if getattr(request, "htmx", False):
        return True
    return request.headers.get("HX-Request") == "true"


def htmx_login_required(view_func):
    """
    Exige login. Em HTMX responde com HX-Redirect para /entrar/
    (volta para a landing apos autenticar).
    """

    @wraps(view_func)
    def _wrapped(request, *args, **kwargs):
        if request.user.is_authenticated:
            return view_func(request, *args, **kwargs)

        # Volta para a vitrine apos login (nao para a URL de POST do carrinho)
        next_url = reverse("loja:landing")
        login_url = f"{reverse('loja:login')}?next={quote(next_url)}"

        if _eh_htmx(request):
            response = HttpResponse(status=204)
            response["HX-Redirect"] = login_url
            return response
        return redirect(login_url)

    return _wrapped
