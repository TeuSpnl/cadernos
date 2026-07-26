import { useEffect, useRef } from "react";

// Campo de texto com foco automático ao montar (estilo Typeform)

export default function CampoTexto({
  id,
  type = "text",
  value,
  onChange,
  onEnter,
  placeholder,
  error,
  autoComplete,
  inputMode,
}) {
  const ref = useRef(null);

  useEffect(() => {
    // Foca o campo ao entrar na pergunta
    const timer = setTimeout(() => ref.current?.focus(), 80);
    return () => clearTimeout(timer);
  }, [id]);

  function handleKeyDown(e) {
    if (e.key === "Enter") {
      e.preventDefault();
      onEnter?.();
    }
  }

  return (
    <div>
      <input
        ref={ref}
        id={id}
        className={`field${error ? " field--error" : ""}`}
        type={type}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder={placeholder}
        autoComplete={autoComplete}
        inputMode={inputMode}
        aria-invalid={Boolean(error)}
        aria-describedby={error ? `${id}-error` : undefined}
      />
      {error ? (
        <p id={`${id}-error`} className="field-error" role="alert">
          {error}
        </p>
      ) : null}
    </div>
  );
}
