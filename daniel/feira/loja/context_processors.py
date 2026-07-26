"""Context processor: badge do carrinho em todos os templates."""

from .models import Carrinho


def carrinho_context(request):
    total = 0
    if request.user.is_authenticated:
        try:
            total = request.user.carrinho.total_itens
        except Carrinho.DoesNotExist:
            total = 0
    return {"carrinho_total_itens": total}
