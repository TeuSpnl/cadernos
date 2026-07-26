import { useEffect, useState } from "react";

/**
 * Alterna a chave de animação para re-disparar a entrada do passo.
 * Comentário: evita estado de "exit" complicado; a troca de step reinicia o CSS.
 */
export function useStepAnimation(step) {
  const [animKey, setAnimKey] = useState(0);

  useEffect(() => {
    setAnimKey((k) => k + 1);
  }, [step]);

  return animKey;
}
