"""
URL configuration do projeto.
Area /painel/ e exclusiva de administradores (is_staff / is_superuser).
"""

from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from django.urls import include, path

urlpatterns = [
    # Admin nativo do Django (tambem exige is_staff)
    path("django-admin/", admin.site.urls),
    # Loja: landing, auth, carrinho, pedidos e painel gerencial
    path("", include("loja.urls")),
]

# Clientes comuns em rotas admin recebem 403 (templates/403.html)
handler403 = "django.views.defaults.permission_denied"

# Em desenvolvimento, servir arquivos das pastas da landing
if settings.DEBUG:
    from django.contrib.staticfiles.urls import staticfiles_urlpatterns

    urlpatterns += staticfiles_urlpatterns()
    urlpatterns += static("/css/", document_root=settings.BASE_DIR / "css")
    urlpatterns += static("/js/", document_root=settings.BASE_DIR / "js")
    urlpatterns += static("/images/", document_root=settings.BASE_DIR / "images")
    urlpatterns += static("/assets/", document_root=settings.BASE_DIR / "assets")
