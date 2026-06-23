"""
Relatório 1 - Produção da OFICINA no mês passado.

Composição da "produção da oficina":
  1) Todas as Ordens de Serviço (OS) efetivadas do mês (valor total com desconto).
     -> Toda OS é da oficina, então não precisa filtrar vendedor ("tranquilo").
  2) As linhas de PEDIDO DE VENDA que sejam peça "aço cromado" ou "tubo",
     de QUALQUER vendedor (essas peças são produção da oficina vendidas no balcão).

Regras:
  - Cliente com "COMAGRO" no nome NÃO entra (nem em OS, nem em pedidos).
  - Aço cromado: casa todas as variações (aco cromado, açocromado, ACO CROMADO...).
  - Tubo: casa só tubo de aço/material (TUBO, CJ TUBO, TUBO DE ACO, KG DE TUBO...),
    excluindo peças automotivas que só têm "tubo" no nome (abraçadeira, conexão,
    luva, junta, filtro, colmeia etc.). Listas configuráveis abaixo.

Período: por padrão, o mês passado (calculado pela data de hoje). Pode passar
um período manual: python relatorio_oficina_acocromado_tubo.py 2026-05-01 2026-05-31
"""
import os
import sys
import calendar
import unicodedata
from datetime import datetime

import firebirdsql
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Pasta de saída (mesmo hábito dos outros relatórios do projeto)
PASTA_SAIDA = "arquivos"

# ------------------------------------------------------------------ #
# CONFIGURAÇÃO DO CASAMENTO DE DESCRIÇÕES (peças da oficina)          #
# ------------------------------------------------------------------ #
# Palavras que, se aparecerem, DESQUALIFICAM um "tubo" (é peça automotiva,
# não o tubo de aço da oficina). Edite à vontade.
TUBO_EXCLUI = [
    "ABRACADEIRA", "CONEXAO", "LUVA", "JUNTA", "FILTRO", "COLMEIA",
    "MANGUEIRA", "PRESILHA", "RADIADOR", "COMBUSTIVEL", "SACA-PINOS",
    "SACA PINOS", "GRAMPO", "BRACADEIRA",
]

# Tokens que, quando iniciam a descrição, indicam que é tubo de aço/material.
TUBO_PREFIXOS_OK = ("TUBO", "CJ", "JG", "JOGO", "CONJUNTO", "KG", "ACO", "SUCATA")


def normalizar(texto: str) -> str:
    """Sobe para maiúsculas e remove acentos para casar de forma robusta."""
    if texto is None:
        return ""
    txt = unicodedata.normalize("NFKD", str(texto))
    txt = txt.encode("ASCII", "ignore").decode("ASCII")
    return txt.upper().strip()


def classificar_peca(descricao: str):
    """
    Retorna 'ACO CROMADO', 'TUBO' ou None conforme a descrição do item.
    """
    norm = normalizar(descricao)
    if not norm:
        return None

    # Aço cromado: removendo espaços, basta conter "ACOCROMAD"
    # (cobre "ACO CROMADO", "AÇO CROMADO", "ACOCROMADO", "AÇO CROMADO 50MM"...).
    sem_espaco = norm.replace(" ", "")
    if "ACOCROMAD" in sem_espaco:
        return "ACO CROMADO"

    # Tubo: precisa conter a palavra TUBO/TUBOS, não pode ter palavra de exclusão,
    # e a descrição precisa "parecer" tubo de aço (começa com prefixo válido
    # ou traz expressões como "DE TUBO" / "DE ACO").
    tem_tubo = "TUBO" in norm  # cobre TUBO e TUBOS
    if not tem_tubo:
        return None

    for proibido in TUBO_EXCLUI:
        if proibido in norm:
            return None

    primeira_palavra = norm.replace(".", " ").split()[0] if norm.split() else ""
    parece_tubo = (
        primeira_palavra.startswith(TUBO_PREFIXOS_OK)
        or "DE TUBO" in norm
        or "DE ACO" in norm
        or "SUCATA TUBO" in norm
    )
    if parece_tubo:
        return "TUBO"
    return None


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


def periodo_mes_passado():
    """(primeiro_dia, ultimo_dia) do mês passado em strings YYYY-MM-DD."""
    hoje = datetime.today()
    if hoje.month == 1:
        ano, mes = hoje.year - 1, 12
    else:
        ano, mes = hoje.year, hoje.month - 1
    ultimo = calendar.monthrange(ano, mes)[1]
    return f"{ano}-{mes:02d}-01", f"{ano}-{mes:02d}-{ultimo:02d}"


def total_ordens_servico(conn, ini, fim):
    """Soma o valor (com desconto) de todas as OS efetivadas do período, sem COMAGRO."""
    sql = """
        SELECT
            O.CDORDEMSERVICO,
            O.DATA,
            O.NOMECLIENTE,
            O.VALORCDESC
        FROM ORDEMSERVICO O
        LEFT JOIN CLIENTE C ON O.CDCLIENTE = C.CDCLIENTE
        WHERE O.EFETIVADO = 'S'
          AND O.DATA BETWEEN ? AND ?
          AND COALESCE(UPPER(O.NOMECLIENTE), '') NOT LIKE '%COMAGRO%'
          AND COALESCE(UPPER(C.NOME), '') NOT LIKE '%COMAGRO%'
        ORDER BY O.DATA, O.CDORDEMSERVICO
    """
    df = pd.read_sql(sql, conn, params=(ini, fim))
    if not df.empty:
        df["VALORCDESC"] = pd.to_numeric(df["VALORCDESC"], errors="coerce").fillna(0.0)
    return df


def itens_pedidos_pecas(conn, ini, fim):
    """
    Traz as linhas de itens de pedido que contêm CROMAD ou TUBO (pré-filtro amplo),
    para classificação fina em Python. Qualquer vendedor, sem COMAGRO.
    """
    sql = """
        SELECT
            P.CDPEDIDOVENDA,
            P.DATA,
            P.CDFUNC,
            F.NOME            AS VENDEDOR,
            P.NOMECLIENTE,
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
          AND (UPPER(I.DESCRICAO) LIKE '%CROMAD%' OR UPPER(I.DESCRICAO) LIKE '%TUBO%')
    """
    df = pd.read_sql(sql, conn, params=(ini, fim))
    if df.empty:
        return df

    df["VALOR_ITEM"] = pd.to_numeric(df["VALOR_ITEM"], errors="coerce").fillna(0.0)
    df["QUANTIDADE"] = pd.to_numeric(df["QUANTIDADE"], errors="coerce").fillna(0.0)
    df["TIPO_PECA"] = df["DESCRICAO"].apply(classificar_peca)
    # Mantém só o que realmente é aço cromado / tubo
    df = df[df["TIPO_PECA"].notna()].reset_index(drop=True)
    return df


def main():
    if len(sys.argv) >= 3:
        ini, fim = sys.argv[1], sys.argv[2]
    else:
        ini, fim = periodo_mes_passado()

    print(f"Período: {ini} a {fim}")
    os.makedirs(PASTA_SAIDA, exist_ok=True)

    conn = get_firebird_connection()
    try:
        df_os = total_ordens_servico(conn, ini, fim)
        df_itens = itens_pedidos_pecas(conn, ini, fim)
    finally:
        conn.close()

    total_os = float(df_os["VALORCDESC"].sum()) if not df_os.empty else 0.0
    qtd_os = len(df_os)

    if df_itens.empty:
        total_aco = total_tubo = 0.0
        resumo_pecas = pd.DataFrame(columns=["TIPO_PECA", "Qtd. linhas", "Valor"])
    else:
        agrup = df_itens.groupby("TIPO_PECA")["VALOR_ITEM"].agg(["count", "sum"])
        total_aco = float(agrup["sum"].get("ACO CROMADO", 0.0))
        total_tubo = float(agrup["sum"].get("TUBO", 0.0))
        resumo_pecas = (
            agrup.reset_index()
            .rename(columns={"count": "Qtd. linhas", "sum": "Valor"})
        )

    total_geral = total_os + total_aco + total_tubo

    # Monta o resumo final (uma linha por componente + total)
    resumo = pd.DataFrame(
        [
            {"Componente": f"Ordens de Serviço (todas) - {qtd_os} OS", "Valor": total_os},
            {"Componente": "Pedidos - peças AÇO CROMADO", "Valor": total_aco},
            {"Componente": "Pedidos - peças TUBO", "Valor": total_tubo},
            {"Componente": "TOTAL PRODUÇÃO OFICINA", "Valor": total_geral},
        ]
    )

    nome_arquivo = os.path.join(
        PASTA_SAIDA, f"oficina_acocromado_tubo_{ini.replace('-', '')}_{fim.replace('-', '')}.xlsx"
    )

    with pd.ExcelWriter(nome_arquivo, engine="openpyxl") as writer:
        resumo.to_excel(writer, sheet_name="Resumo", index=False)
        resumo_pecas.to_excel(writer, sheet_name="Resumo Peças (Pedidos)", index=False)
        if not df_itens.empty:
            cols = [
                "TIPO_PECA", "CDPEDIDOVENDA", "DATA", "VENDEDOR", "NOMECLIENTE",
                "DESCRICAO", "QUANTIDADE", "VALOR_ITEM",
            ]
            df_itens[cols].to_excel(writer, sheet_name="Itens Pedidos", index=False)
        if not df_os.empty:
            df_os.to_excel(writer, sheet_name="OS do Mês", index=False)

    print("\n===== RESUMO =====")
    print(f"OS (todas, {qtd_os} OS): R$ {total_os:,.2f}")
    print(f"Pedidos AÇO CROMADO:     R$ {total_aco:,.2f}")
    print(f"Pedidos TUBO:            R$ {total_tubo:,.2f}")
    print(f"TOTAL OFICINA:           R$ {total_geral:,.2f}")
    print(f"\nPlanilha gerada: {nome_arquivo}")


if __name__ == "__main__":
    main()
