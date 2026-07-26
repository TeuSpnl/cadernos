// Comportamentos da landing — lightbox + reveal (carrinho via HTMX)
document.addEventListener("DOMContentLoaded", () => {
  // Ano automático no rodapé
  const yearEl = document.getElementById("year");
  if (yearEl) {
    yearEl.textContent = String(new Date().getFullYear());
  }

  // Reveal dos cards ao entrar na viewport
  const cards = document.querySelectorAll("[data-reveal]");
  if ("IntersectionObserver" in window) {
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            entry.target.classList.add("is-visible");
            observer.unobserve(entry.target);
          }
        });
      },
      { threshold: 0.12, rootMargin: "0px 0px -20px 0px" }
    );

    cards.forEach((card, index) => {
      card.style.transitionDelay = `${index * 100}ms`;
      observer.observe(card);
    });
  } else {
    cards.forEach((card) => card.classList.add("is-visible"));
  }

  // Feedback visual no botão comprar / adicionar
  document.querySelectorAll(".btn-buy").forEach((btn) => {
    btn.addEventListener("click", () => {
      btn.classList.add("is-pressed");
      window.setTimeout(() => btn.classList.remove("is-pressed"), 180);
    });
  });

  // ---------- Lightbox ----------
  const lightbox = document.getElementById("lightbox");
  const lightboxImg = document.getElementById("lightbox-img");
  const lightboxClose = document.getElementById("lightbox-close");
  let lastFocus = null;

  function openLightbox(img) {
    if (!lightbox || !lightboxImg || !img) return;
    lastFocus = document.activeElement;
    lightboxImg.src = img.currentSrc || img.src;
    lightboxImg.alt = img.alt || "Foto ampliada da camiseta";
    lightbox.hidden = false;
    document.body.classList.add("lightbox-open");
    lightboxClose?.focus();
  }

  function closeLightbox() {
    if (!lightbox || !lightboxImg) return;
    lightbox.hidden = true;
    lightboxImg.removeAttribute("src");
    lightboxImg.alt = "";
    document.body.classList.remove("lightbox-open");
    if (lastFocus && typeof lastFocus.focus === "function") {
      lastFocus.focus();
    }
  }

  document.querySelectorAll(".product-card__zoom").forEach((btn) => {
    btn.addEventListener("click", (event) => {
      event.stopPropagation();
      openLightbox(btn.querySelector("img"));
    });
  });

  lightboxClose?.addEventListener("click", (event) => {
    event.stopPropagation();
    closeLightbox();
  });

  lightbox?.addEventListener("click", (event) => {
    if (event.target === lightbox) closeLightbox();
  });

  lightboxImg?.addEventListener("click", (event) => event.stopPropagation());

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && lightbox && !lightbox.hidden) {
      closeLightbox();
    }
  });
});
