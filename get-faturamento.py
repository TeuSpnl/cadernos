import os
import firebirdsql
import pandas as pd
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

def get_firebird_connection():
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

def gerar_relatorio_consolidado(id_vendedor_pedidos, ano=2025):
    """
    Gera um relatório consolidado somando TODAS as OS da oficina com os 
    PEDIDOS de um vendedor específico, identificado pelo seu CÓDIGO.
    """
    print(f"Iniciando relatório consolidado: Todas as OS + Pedidos do usuário ID '{id_vendedor_pedidos}'...")
    
    conn = get_firebird_connection()
    if not conn:
        return

    # Definindo o período para Janeiro a Junho
    start_date = f'{ano}-01-01'
    end_date = f'{ano}-06-30'
    
    # Query SQL Híbrida:
    # 1. Pega TODAS as Ordens de Serviço (OS).
    # 2. Pega APENAS os Pedidos de Venda do vendedor com o código especificado.
    query = """
        -- Parte 1: Todas as Ordens de Serviço de todos os vendedores
        SELECT 
            o.DATA AS DATA_VENDA,
            o.VALORCDESC AS VALOR_VENDA
        FROM ORDEMSERVICO o
        WHERE 
            o.EFETIVADO = 'S'
            AND UPPER(o.NOMECLIENTE) NOT LIKE '%COMAGRO%'
            AND o.DATA BETWEEN ? AND ?
            
        UNION ALL

        -- Parte 2: Apenas os Pedidos de Venda do usuário com o ID especificado
        SELECT 
            p.DATA AS DATA_VENDA,
            p.VALORCDESC AS VALOR_VENDA
        FROM PEDIDOVENDA p
        JOIN FUNCIONARIO f ON p.CDFUNC = f.CDFUNC
        WHERE 
            p.EFETIVADO = 'S'
            AND p.DEVOLVIDO <> 'S'
            AND UPPER(p.NOMECLIENTE) NOT LIKE '%COMAGRO%'
            AND f.CDFUNC = ? -- ALTERADO DE f.NOME PARA f.CDFUNC
            AND p.DATA BETWEEN ? AND ?
    """

    try:
        print(f"Buscando dados de {start_date} a {end_date}...")
        
        # Os parâmetros devem seguir a ordem exata dos '?' na query
        params = (start_date, end_date, id_vendedor_pedidos, start_date, end_date)
        df = pd.read_sql(query, conn, params=params)
        
        print(f"Foram encontrados {len(df)} registros no total (OS + Pedidos).")

        if df.empty:
            print("Nenhum dado encontrado para esta combinação. O relatório não será gerado.")
            return

        # --- O Processamento dos Dados continua o mesmo ---
        
        df['DATA_VENDA'] = pd.to_datetime(df['DATA_VENDA'])
        df['VALOR_VENDA'] = pd.to_numeric(df['VALOR_VENDA'], errors='coerce').fillna(0)

        meses_pt = {
            1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
            5: 'Maio', 6: 'Junho'
        }
        df['Mês'] = df['DATA_VENDA'].dt.month.map(meses_pt)
        
        faturamento_mensal = df.groupby('Mês')['VALOR_VENDA'].sum().reset_index()
        faturamento_mensal = faturamento_mensal.rename(columns={'VALOR_VENDA': 'Faturamento Total'})
        
        faturamento_mensal['Mês_Num'] = pd.Categorical(faturamento_mensal['Mês'], categories=meses_pt.values(), ordered=True)
        faturamento_mensal = faturamento_mensal.sort_values('Mês_Num').drop(columns=['Mês_Num'])

        total_geral = faturamento_mensal['Faturamento Total'].sum()
        total_df = pd.DataFrame([{'Mês': 'TOTAL GERAL', 'Faturamento Total': total_geral}])
        resultado_final = pd.concat([faturamento_mensal, total_df], ignore_index=True)

        # --- Exportação para Excel ---
        
        nome_arquivo = f'relatorio_consolidado_oficina_e_vendedor_{id_vendedor_pedidos}.xlsx'
        writer = pd.ExcelWriter(nome_arquivo, engine='openpyxl')
        resultado_final.to_excel(writer, sheet_name='Faturamento Consolidado', index=False)

        workbook = writer.book
        worksheet = writer.sheets['Faturamento Consolidado']
        
        worksheet.column_dimensions['A'].width = 20
        worksheet.column_dimensions['B'].width = 25
        
        currency_format = 'R$ #,##0.00'
        for row in range(2, len(resultado_final) + 2):
            worksheet[f'B{row}'].number_format = currency_format

        writer.close()
        
        print(f"\nRelatório '{nome_arquivo}' gerado com sucesso!")

    except Exception as e:
        print(f"Ocorreu um erro durante a execução: {e}")
    finally:
        print("Fechando conexão com o banco de dados.")
        conn.close()


if __name__ == '__main__':
    # Agora passamos o CÓDIGO do usuário para filtrar os pedidos
    gerar_relatorio_consolidado(id_vendedor_pedidos=2621, ano=2025)