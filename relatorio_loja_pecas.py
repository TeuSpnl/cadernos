"""
Relatório 2 - Peças vendidas pela LOJA (jan/2024 a maio/2026).

O que conta como venda da LOJA:
  - Pedidos de venda (PEDIDOVENDA) efetivados, não devolvidos.
  - Apenas as linhas de PEÇA (produto). Linhas de serviço são ignoradas.
  - Vendedor NÃO pode ser da oficina:
      * Paulo (CDFUNC 28) e Clériston Spínola (CDFUNC 4): oficina o tempo todo -> excluídos.
      * Palmiro (CDFUNC 42): mudou de filial em 09/2025.
          - Antes de 01/09/2025: era da LOJA  -> ENTRA no relatório.
          - A partir de 01/09/2025: virou oficina -> NÃO entra.
  - Cliente com "COMAGRO" no nome NÃO entra.

Período: por padrão 2024-01-01 a 2026-05-31. Pode passar manual:
    python relatorio_loja_pecas.py 2024-01-01 2026-05-31
"""
import os
import sys

import firebirdsql
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

PASTA_SAIDA = "arquivos"

# ------------------------------------------------------------------ #
# REGRA DE VENDEDORES DA OFICINA (a EXCLUIR do relatório da loja)     #
# ------------------------------------------------------------------ #
# Sempre oficina (excluir o período todo):
OFICINA_SEMPRE = [4, 36]            # 4 = CLERISTON SALINAS SPINOLA, 36 = JOÃO PAULO SANTANA BATISTA
# Palmiro mudou de filial; vira oficina a partir desta data (vendas >= data saem):
PALMIRO_CDFUNC = 42
PALMIRO_VIRA_OFICINA_EM = "2025-09-01"

# Período padrão
PERIODO_INI_PADRAO = "2024-01-01"
PERIODO_FIM_PADRAO = "2026-05-31"


def get_firebird_connection():
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


def buscar_pecas_loja(conn, ini, fim):
    """
    Itens (peças) de pedidos de venda da loja no período.
    A regra de vendedor da oficina é aplicada direto no SQL:
      - exclui CDFUNC em OFICINA_SEMPRE;
      - exclui Palmiro só quando a venda é >= data em que ele virou oficina.
    """
    placeholders = ",".join("?" for _ in OFICINA_SEMPRE)

    sql = f"""
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
        LEFT JOIN CLIENTE C ON P.CDCLIENTE = C.CDCLIENTE
        WHERE P.EFETIVADO = 'S'
          AND COALESCE(P.DEVOLVIDO, 'N') <> 'S'
          AND P.DATA BETWEEN ? AND ?
          AND COALESCE(UPPER(P.NOMECLIENTE), '') NOT LIKE '%COMAGRO%'
          AND COALESCE(UPPER(C.NOME), '') NOT LIKE '%COMAGRO%'
          AND I.CDPRODUTO IS NOT NULL          -- só peças (produto), ignora serviço
          AND P.CDFUNC NOT IN ({placeholders}) -- oficina o tempo todo
          AND NOT (P.CDFUNC = ? AND P.DATA >= ?)  -- Palmiro só depois que virou oficina
        ORDER BY P.DATA, P.CDPEDIDOVENDA
    """

    params = (
        ini, fim,
        *OFICINA_SEMPRE,
        PALMIRO_CDFUNC, PALMIRO_VIRA_OFICINA_EM,
    )
    df = pd.read_sql(sql, conn, params=params)
    if not df.empty:
        df["VALOR_ITEM"] = pd.to_numeric(df["VALOR_ITEM"], errors="coerce").fillna(0.0)
        df["QUANTIDADE"] = pd.to_numeric(df["QUANTIDADE"], errors="coerce").fillna(0.0)
        df["DATA"] = pd.to_datetime(df["DATA"])
    return df


def main():
    if len(sys.argv) >= 3:
        ini, fim = sys.argv[1], sys.argv[2]
    else:
        ini, fim = PERIODO_INI_PADRAO, PERIODO_FIM_PADRAO

    print(f"Período: {ini} a {fim}")
    os.makedirs(PASTA_SAIDA, exist_ok=True)

    conn = get_firebird_connection()
    try:
        df = buscar_pecas_loja(conn, ini, fim)
    finally:
        conn.close()

    print(f"Linhas de peças encontradas: {len(df)}")

    base = f"loja_pecas_{ini.replace('-', '')}_{fim.replace('-', '')}"
    caminho_xlsx = os.path.join(PASTA_SAIDA, f"{base}.xlsx")
    caminho_csv = os.path.join(PASTA_SAIDA, f"{base}_detalhe.csv")

    if df.empty:
        # Nada encontrado: gera planilha vazia só para registro
        pd.DataFrame(columns=["Sem dados"]).to_excel(caminho_xlsx, index=False)
        print("Nenhuma peça encontrada para o período/regra. Planilha vazia gerada.")
        return

    # Resumos
    df["Mês"] = df["DATA"].dt.to_period("M").astype(str)

    resumo_mensal = (
        df.groupby("Mês", as_index=False)
        .agg(Qtd_Linhas=("VALOR_ITEM", "size"), Valor=("VALOR_ITEM", "sum"))
        .sort_values("Mês")
    )

    resumo_vendedor = (
        df.groupby(["CDFUNC", "VENDEDOR"], as_index=False)
        .agg(Qtd_Linhas=("VALOR_ITEM", "size"), Valor=("VALOR_ITEM", "sum"))
        .sort_values("Valor", ascending=False)
    )

    resumo_produto = (
        df.groupby(["NUMORIGINAL", "DESCRICAO"], as_index=False)
        .agg(Qtd=("QUANTIDADE", "sum"), Valor=("VALOR_ITEM", "sum"))
        .sort_values("Valor", ascending=False)
        .head(500)  # top 500 produtos para não estourar a aba
    )

    total_geral = pd.DataFrame(
        [{"Descrição": "TOTAL GERAL (peças loja)", "Valor": float(df["VALOR_ITEM"].sum())}]
    )

    # Detalhe completo vai para CSV (robusto p/ qualquer volume e abre no Excel)
    cols_detalhe = [
        "CDPEDIDOVENDA", "DATA", "CDFUNC", "VENDEDOR", "NOMECLIENTE",
        "CDPRODUTO", "NUMORIGINAL", "DESCRICAO", "QUANTIDADE", "VALOR_ITEM",
    ]
    df[cols_detalhe].to_csv(caminho_csv, index=False, sep=";", encoding="utf-8-sig")

    with pd.ExcelWriter(caminho_xlsx, engine="openpyxl") as writer:
        total_geral.to_excel(writer, sheet_name="Total", index=False)
        resumo_mensal.to_excel(writer, sheet_name="Resumo Mensal", index=False)
        resumo_vendedor.to_excel(writer, sheet_name="Resumo Vendedor", index=False)
        resumo_produto.to_excel(writer, sheet_name="Top Produtos", index=False)
        # Se o detalhe couber com folga no Excel, inclui também numa aba
        if len(df) <= 500_000:
            df[cols_detalhe].to_excel(writer, sheet_name="Detalhe", index=False)

    print("\n===== RESUMO LOJA =====")
    print(f"TOTAL peças loja: R$ {float(df['VALOR_ITEM'].sum()):,.2f}")
    print(f"Planilha: {caminho_xlsx}")
    print(f"Detalhe (CSV): {caminho_csv}")


if __name__ == "__main__":
    main()
