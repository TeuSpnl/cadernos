// Barra de progresso do formulário (amarelo mostarda)

export default function ProgressBar({ atual, total }) {
  const percentual = Math.round((atual / total) * 100);

  return (
    <div>
      <div
        className="progress"
        role="progressbar"
        aria-valuenow={percentual}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-label="Progresso da inscrição"
      >
        <div className="progress__bar" style={{ width: `${percentual}%` }} />
      </div>
      <p className="progress__label">
        Passo {atual} de {total}
      </p>
    </div>
  );
}
