from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import firebirdsql
import sys
import os

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# --- CONFIGURAÇÕES DO BANCO DE DADOS ---
def get_firebird_connection():
    try:
        return firebirdsql.connect(
            host=os.getenv('HOST'),
            port=int(os.getenv('PORT', '3050')),
            database=os.getenv('DB_PATH'),
            user=os.getenv('APP_USER'),
            password=os.getenv('PASSWORD'),
            role=os.getenv('ROLE'),
            auth_plugin_name=os.getenv('AUTH'),
            wire_crypt=False,
            charset='ISO8859_1'
        )
    except Exception as e:
        print(f"Erro CRÍTICO ao tentar conectar ao banco de dados: {e}")
        print("Verifique as variáveis de ambiente no seu arquivo .env (HOST, DB_PATH, APP_USER, etc.)")
        return None

# Nome do arquivo de saída
OUTPUT_FILE = 'relatorios_de_estoque_e_clientes.xlsx'

def main():
    """
    Função principal que conecta ao banco, executa as consultas
    e salva os resultados em um arquivo Excel com várias abas.
    """
    conn = None
    try:
        print("Tentando conectar ao banco de dados Firebird...")
        conn = get_firebird_connection()
        if not conn:
            sys.exit(1)
            
        print("Conexão estabelecida com sucesso.")

        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
            
            # --- COLUNAS ESPECÍFICAS PARA CLIENTES ---
            colunas_cliente = """
                CDCLIENTE, NOME, CIDADE, ESTADO, CEP, DTCADASTRO, DTULTIMOMOV, 
                PRAZOPGTO, CDTIPO, CDDESCONTO, TIPO, PRAZOPEDIDO, LIMITECREDITO, 
                SITUACAO, CPF_CNPJ, NOMEFANTASIA, RESPONSAVELPGTO, FONERESPPGTO, 
                RESPONSAVELCOMPRA, FONERESPCOMPRA, IE, UF_RG, DIASPRAZOPAG, APELIDO
            """

            # 1. Clientes Ativos
            print("Extraindo clientes ativos (colunas selecionadas)...")
            sql_clientes_ativos = f"SELECT {colunas_cliente} FROM CLIENTE WHERE SITUACAO = 'ATIVO' ORDER BY NOME"
            df_clientes_ativos = pd.read_sql_query(sql_clientes_ativos, conn)
            df_clientes_ativos.to_excel(writer, sheet_name='Clientes Ativos', index=False)
            print(f"-> {len(df_clientes_ativos)} clientes ativos encontrados.")

            # 2. Clientes Inativos e Outros
            print("Extraindo clientes inativos e outros (colunas selecionadas)...")
            sql_clientes_inativos = f"SELECT {colunas_cliente} FROM CLIENTE WHERE SITUACAO <> 'ATIVO' OR SITUACAO IS NULL ORDER BY NOME"
            df_clientes_inativos = pd.read_sql_query(sql_clientes_inativos, conn)
            df_clientes_inativos.to_excel(writer, sheet_name='Clientes Inativos e Outros', index=False)
            print(f"-> {len(df_clientes_inativos)} clientes com outros status encontrados.")

            # --- LÓGICA DE ESTOQUE ATUALIZADA (SEM JOIN) ---
            
            print("Extraindo relatório de estoque completo...")
            colunas_estoque_completo = """
                CDPRODUTO, NUMORIGINAL, FORNECEDOR, MARCA, DESCRICAO, ESTOQUEPREVISTO, 
                PRECOVENDA, UNIDADE, CDAPLICACAO, ULTIMOCUSTO, ULTIMOCUSTO1, CDICMS, CDIPI, ESTIDEAL, 
                PRECOMINIMO, ULTDTSAIDA, ULTDTENT, QUANTIDADEEMB, PESOBRUTO, PESOLIQ, 
                LISTA_COMPARATIVA, CORRIGIDO, QUANTENTRADA, PVENDANOTA, ULTPRECOVENDA, 
                ULTPRECOVENDA1, CDICMSFORA, ULTPRECOMINIMO, ULTPRECOMINIMO1, PRECOMEDIO, 
                TIPO_ULTIMA_MOVIMENTACAO, PRECOCUSTO_CRIPTOGRAFADO, ESTOQUEINICIAL, 
                DTESTOQUEINICIAL, PRECOCOMPRADO, VALICMSCOMPRA, VALIPICOMPRA, VALFRETECOMPRA, 
                VALOUTROCOMPRA, IMOBILIZADO, VALIMPOSTOVENDA, VALLUCRO, PRECOCUSTOFISCAL, 
                PRECOCUSTOREALNOTA, NCM, PAF_CODIGO_FABRICA, CEST, NUMREFERENCIA, LOCALIZACAO, OBS_HIST
            """
            
            sql_estoque_completo_query = f"""
                SELECT {colunas_estoque_completo} 
                FROM PRODUTO 
                ORDER BY NUMORIGINAL
            """
            df_estoque_completo = pd.read_sql_query(sql_estoque_completo_query, conn)
            
            # 3. Planilha: Estoque Completo
            df_estoque_completo.to_excel(writer, sheet_name='Estoque Completo', index=False)
            print(f"-> {len(df_estoque_completo)} produtos totais encontrados.")
            
            # 4. Planilha: Estoque para Vendas
            print("Filtrando e formatando o estoque para a planilha de vendas...")
            
            df_estoque_vendas = df_estoque_completo[df_estoque_completo['ESTOQUEPREVISTO'] > 0].copy()
            
            colunas_para_vendas = [
                'CDPRODUTO', 'NUMORIGINAL', 'FORNECEDOR', 'MARCA', 
                'DESCRICAO', 'ESTOQUEPREVISTO', 'PRECOVENDA', 'UNIDADE'
            ]
            
            df_estoque_vendas_final = df_estoque_vendas[colunas_para_vendas].rename(
                columns={'FORNECEDOR': 'FORNECEDOR'}
            )
            
            df_estoque_vendas_final.to_excel(writer, sheet_name='Estoque para Vendas', index=False)
            print(f"-> {len(df_estoque_vendas_final)} produtos com estoque positivo formatados para vendas.")


            # 5. Histórico de Produtos (ÚLTIMO 1 MÊS)
            print("Extraindo histórico de produtos do último mês... (Isso pode demorar um pouco)")
            
            data_limite = datetime.now() - relativedelta(months=1) # Alterado de 6 para 1
            data_limite_str = data_limite.strftime('%Y-%m-%d')
            
            union_queries = []
            for i in range(1, 11):
                union_queries.append(f"SELECT * FROM HISTORICOPRODUTO{i} WHERE DATA >= '{data_limite_str}'")
            
            sql_historico_base = " UNION ALL ".join(union_queries)

            sql_historico_completo = f"""
                SELECT
                    p.NUMORIGINAL,
                    h.*
                FROM
                    ({sql_historico_base}) h
                JOIN
                    PRODUTO p ON h.CDPRODUTO = p.CDPRODUTO
                ORDER BY
                    h.CDPRODUTO, h.DATA
            """
            
            df_historico = pd.read_sql_query(sql_historico_completo, conn)
            df_historico.to_excel(writer, sheet_name='Histórico de Produtos', index=False)
            print(f"-> {len(df_historico)} registros de histórico encontrados.")

        print(f"\nOperação concluída com sucesso! O arquivo '{OUTPUT_FILE}' foi gerado.")

    except firebirdsql.OperationalError as e:
        print(f"Erro de operação do Firebird: {e}")
        print("Isso pode ser um erro na sua consulta SQL (verifique se todos os nomes de colunas estão corretos) ou um problema de conexão/permissão.")
        sys.exit(1)
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            print("Conexão com o banco de dados fechada.")

if __name__ == '__main__':
    main()