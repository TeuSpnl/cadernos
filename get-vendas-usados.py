"""
Relatório de peças USADAS vendidas, separadas mês a mês.

Filtra itens de pedidos de venda (PEDIDOVENDA) efetivados e não devolvidos
cuja descrição contenha "USADO". A separação mensal usa a data do pedido.

Uso:
    python pecas_usadas_vendidas.py                      # período padrão
    python pecas_usadas_vendidas.py 2024-01-01 2026-06-30  # período manual
"""
import os
import sys

import firebirdsql
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

PASTA_SAIDA = "arquivos"

# Período padrão — ajuste conforme necessidade
PERIODO_INI_PADRAO = "2026-04-01"
PERIODO_FIM_PADRAO = "2026-06-30"


def get_firebird_connection():
    """Conexão Firebird usando as variáveis do .env."""
    return firebirdsql.connect(
        host=os.getenv("HOST"),
        port=int(os.getenv("PORT", "3050")),
        database=os.getenv("DB_PATH"),
        user=os.getenv("APP_USER"),
        password=os.getenv("PASSWORD"),
        role=os.getenv("ROLE"),
        auth_plugin_name=os.getenv("AUTH"),
        wire_crypt=False,
        charset="ISO8859_1",
    )


def buscar_pecas_usadas(conn, ini, fim):
    """
    Retorna todas as peças vendidas cuja descrição contém 'USADO',
    em pedidos efetivados e não devolvidos dentro do período.
    """
    sql = """
        SELECT
            P.CDPEDIDOVENDA,
            P.DATA,
            P.CDFUNC,
            F.NOME            AS VENDEDOR,
            P.NOMECLIENTE,
            I.CDPRODUTO,
            I.NUMORIGINAL,
            I.DESCRICAO,
            I.QUANTIDADE,
            COALESCE(I.VALORCDESCREAL, I.VALORCDESC, I.VALORTOTAL) AS VALOR_ITEM
        FROM ITENSPEDIDOVENDA I
        JOIN PEDIDOVENDA P ON I.CDPEDIDOVENDA = P.CDPEDIDOVENDA
        LEFT JOIN FUNCIONARIO F ON P.CDFUNC = F.CDFUNC
        WHERE P.EFETIVADO = 'S'
          AND COALESCE(P.DEVOLVIDO, 'N') <> 'S'
          AND P.DATA BETWEEN ? AND ?
          AND I.CDPRODUTO IS NOT NULL
          AND UPPER(I.DESCRICAO) LIKE '%USADO%'
        ORDER BY P.DATA, P.CDPEDIDOVENDA
    """
    df = pd.read_sql(sql, conn, params=(ini, fim))

    if not df.empty:
        df["VALOR_ITEM"] = pd.to_numeric(df["VALOR_ITEM"], errors="coerce").fillna(0.0)
        df["QUANTIDADE"] = pd.to_numeric(df["QUANTIDADE"], errors="coerce").fillna(0.0)
        df["DATA"] = pd.to_datetime(df["DATA"])

    return df


def main():
    # Período via argumentos ou padrão
    if len(sys.argv) >= 3:
        ini, fim = sys.argv[1], sys.argv[2]
    else:
        ini, fim = PERIODO_INI_PADRAO, PERIODO_FIM_PADRAO

    print(f"Período: {ini} a {fim}")
    os.makedirs(PASTA_SAIDA, exist_ok=True)

    conn = get_firebird_connection()
    try:
        df = buscar_pecas_usadas(conn, ini, fim)
    finally:
        conn.close()

    print(f"Peças usadas encontradas: {len(df)}")

    if df.empty:
        print("Nenhuma peça usada vendida no período.")
        return

    # Coluna auxiliar para agrupar por mês (ex.: "2024-01", "2024-02" …)
    df["MES_REF"] = df["DATA"].dt.to_period("M").astype(str)

    # ── Resumo mensal (console) ──────────────────────────────────────
    resumo = (
        df.groupby("MES_REF", as_index=False)
        .agg(
            Qtd_Itens=("QUANTIDADE", "sum"),
            Qtd_Linhas=("VALOR_ITEM", "size"),
            Valor_Total=("VALOR_ITEM", "sum"),
        )
        .sort_values("MES_REF")
    )

    print("\n===== PEÇAS USADAS VENDIDAS — MÊS A MÊS =====\n")
    for _, row in resumo.iterrows():
        print(
            f"  {row['MES_REF']}  |  "
            f"{int(row['Qtd_Linhas']):>4} linhas  |  "
            f"Qtd: {row['Qtd_Itens']:>8.0f}  |  "
            f"Valor: R$ {row['Valor_Total']:>12,.2f}"
        )

    total = df["VALOR_ITEM"].sum()
    print(f"\n  TOTAL GERAL: R$ {total:,.2f}")

    # ── Exportação para Excel (uma aba por mês + resumo) ─────────────
    base = f"pecas_usadas_{ini.replace('-', '')}_{fim.replace('-', '')}"
    caminho_xlsx = os.path.join(PASTA_SAIDA, f"{base}.xlsx")

    cols_detalhe = [
        "CDPEDIDOVENDA", "DATA", "CDFUNC", "VENDEDOR", "NOMECLIENTE",
        "CDPRODUTO", "NUMORIGINAL", "DESCRICAO", "QUANTIDADE", "VALOR_ITEM",
    ]

    with pd.ExcelWriter(caminho_xlsx, engine="openpyxl") as writer:
        # Aba de resumo mensal
        resumo.to_excel(writer, sheet_name="Resumo Mensal", index=False)

        # Uma aba para cada mês com o detalhe das peças
        for mes, grupo in df.groupby("MES_REF"):
            # Nome da aba: formato curto (ex.: "2024-01")
            nome_aba = str(mes)[:10]  # Excel limita a 31 chars, mas "2024-01" é curto
            grupo[cols_detalhe].to_excel(writer, sheet_name=nome_aba, index=False)

    print(f"\nPlanilha gerada: {caminho_xlsx}")


if __name__ == "__main__":
    main()
