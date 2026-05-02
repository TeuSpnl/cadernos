"""
Consulta vendas efetivadas de HOJE no Firebird do Séculos:
- Pedidos de venda (PEDIDOVENDA)
- Ordens de serviço (ORDEMSERVICO), mesma lógica de colunas
- Recebimentos avulsos (RECEBIMENTO com PEDIDO = 'N'), filtrados pela data de pagamento

Exporta um único Excel ordenado alfabeticamente pela descrição da forma de pagamento (FORMAPAG).
"""
import os
from datetime import datetime

import firebirdsql
import pandas as pd

#########################
# CONFIGURAÇÕES GERAIS  #
# Mesmo padrão de get-vendas.py (conexão idêntica ao Seculos)
#########################

HOST = '100.64.1.10'
PORT = 3050
DATABASE = r'C:\Micromais\mmSeculos\BD\SECULOS.MMDB'
USER = 'USER_CONSULTA'
PASSWORD = 'consultaseculos'
ROLE = 'apenasconsulta'
CHARSET = 'ISO8859_1'

# Pasta de saída (mesmo hábito do relatório de vendas por vendedor)
PASTA_SAIDA = 'arquivos'


def get_firebird_connection():
    """Abre conexão Firebird do Séculos (parâmetros alinhados ao get-vendas.py)."""
    return firebirdsql.connect(
        host=HOST,
        port=PORT,
        database=DATABASE,
        user=USER,
        password=PASSWORD,
        role=ROLE,
        auth_plugin_name='Legacy_Auth',
        wire_crypt=False,
        charset=CHARSET,
    )


def data_hoje_iso():
    """Data local de hoje no formato SQL YYYY-MM-DD."""
    return datetime.today().date().strftime('%Y-%m-%d')


def carregar_movimentos_do_dia(conn, data_iso: str) -> pd.DataFrame:
    """
    Monta um único DataFrame com pedidos, OS e recebimentos do dia.
    Colunas unificadas para permitir ordenação por forma de pagamento.
    """
    # Pedido de venda: join direto em FORMAPAG para trazer a descrição da forma de pgto
    sql_pedido = """
        SELECT
            'PEDIDO DE VENDA' AS TIPO,
            P.CDPEDIDOVENDA AS CODIGO,
            P.NOMECLIENTE AS NOME_CLIENTE,
            P.DATA AS DATA_MOVIMENTO,
            P.VALORCDESC AS VALOR,
            F.DESCRICAO AS FORMA_PAGAMENTO
        FROM PEDIDOVENDA P
        JOIN FORMAPAG F ON P.CDFORMAPAG = F.CDFORMAPAG
        WHERE P.DATA = ?
          AND P.EFETIVADO = 'S'
    """

    # Ordem de serviço: mesma estrutura conceitual do pedido (cliente, valor com desconto, forma)
    sql_os = """
        SELECT
            'ORDEM DE SERVIÇO' AS TIPO,
            O.CDORDEMSERVICO AS CODIGO,
            O.NOMECLIENTE AS NOME_CLIENTE,
            O.DATA AS DATA_MOVIMENTO,
            O.VALORCDESC AS VALOR,
            F.DESCRICAO AS FORMA_PAGAMENTO
        FROM ORDEMSERVICO O
        JOIN FORMAPAG F ON O.CDFORMAPAG = F.CDFORMAPAG
        WHERE O.DATA = ?
          AND O.EFETIVADO = 'S'
    """

    # Recebimento não ligado a pedido: cliente vem da tabela CLIENTE; filtro pelo dia do pagamento
    sql_receb = """
        SELECT
            'RECEBIMENTO (PEDIDO=N)' AS TIPO,
            R.CDRECEBIMENTO AS CODIGO,
            C.NOME AS NOME_CLIENTE,
            R.DTPAGAMENTO AS DATA_MOVIMENTO,
            R.VALORTOTAL AS VALOR,
            F.DESCRICAO AS FORMA_PAGAMENTO
        FROM RECEBIMENTO R
        JOIN FORMAPAG F ON R.CDFORMAPAG = F.CDFORMAPAG
        LEFT JOIN CLIENTE C ON R.CDCLIENTE = C.CDCLIENTE
        WHERE R.PEDIDO = 'N'
          AND CAST(R.DTPAGAMENTO AS DATE) = ?
    """

    partes = []

    for sql in (sql_pedido, sql_os):
        df = pd.read_sql(sql, conn, params=(data_iso,))
        partes.append(df)

    df_rec = pd.read_sql(sql_receb, conn, params=(data_iso,))
    partes.append(df_rec)

    if all(p.empty for p in partes):
        return pd.DataFrame(
            columns=[
                'TIPO',
                'CODIGO',
                'NOME_CLIENTE',
                'DATA_MOVIMENTO',
                'VALOR',
                'FORMA_PAGAMENTO',
            ]
        )

    df_final = pd.concat(partes, ignore_index=True)

    # Normalizações leves para Excel e para ordenação estável
    df_final['VALOR'] = pd.to_numeric(df_final['VALOR'], errors='coerce')
    df_final['FORMA_PAGAMENTO'] = (
        df_final['FORMA_PAGAMENTO'].astype(str).str.strip()
    )
    df_final['NOME_CLIENTE'] = df_final['NOME_CLIENTE'].astype(str).str.strip()

    # Ordem alfabética pela forma de pagamento; desempate por tipo e código
    df_final = df_final.sort_values(
        by=['FORMA_PAGAMENTO', 'TIPO', 'CODIGO'],
        ascending=[True, True, True],
        kind='mergesort',
    ).reset_index(drop=True)

    return df_final


def main():
    data_iso = data_hoje_iso()
    caminho = os.path.join(
        PASTA_SAIDA, f'vendas_sec_hoje_{data_iso.replace("-", "")}.xlsx'
    )

    os.makedirs(PASTA_SAIDA, exist_ok=True)

    try:
        conn = get_firebird_connection()
    except firebirdsql.Error as e:
        print(f'Erro ao conectar ao Firebird: {e}')
        raise

    try:
        df = carregar_movimentos_do_dia(conn, data_iso)
        df.to_excel(caminho, index=False, sheet_name='Vendas do dia')
        print(f'Planilha gerada: {caminho} ({len(df)} linhas, data ref. {data_iso}).')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
