import pandas as pd
import glob
import os

def buscar_descricao_servico(numero_os):
    """
    Busca a descrição do serviço em todos os arquivos EXCEL de serviços.
    """
    # Lista todos os arquivos que seguem o padrão de nome
    arquivos = glob.glob("/Users/mateusspinola/Desktop/RELAÇÃO DE SERVIÇOS 2020.2021.xlsx")
    
    if not arquivos:
        return "Nenhum arquivo encontrado. Verifique se os EXCELs estão na mesma pasta."

    lista_df = []
    
    for arquivo in arquivos:
        # Lê todas as abas do arquivo Excel, pulando a primeira linha de cada aba
        # sheet_name=None retorna um dicionário de DataFrames, onde as chaves são os nomes das abas
        todas_as_abas = pd.read_excel(arquivo, sheet_name=None, skiprows=1)
        
        # Itera sobre cada DataFrame no dicionário (cada aba)
        for sheet_name, df_aba in todas_as_abas.items():
            # Garante que as colunas de busca sejam numéricas para evitar erros de comparação
            # A coluna é '0S.' (com zero) e a quantidade é 'QUAT.'
            df_aba['0S.'] = (df_aba['0S.'].astype(str).str.strip()).str.split('.').str[0]  # Remove o ponto e mantém apenas o número da OS
            df_aba['QUAT.'] = pd.to_numeric(df_aba['QUAT.'], errors='coerce')
            # O debug print pode ser ajustado para mostrar de qual aba a informação vem, se necessário
            # print(f"Debug da aba '{sheet_name}': {df_aba['0S.'].iloc[-10:-1]}")
            # print(f"Debug da aba '{sheet_name}': {df_aba['DESCRIÇÃO DO SERVIÇO'].iloc[-10:-1]}")
            
            lista_df.append(df_aba)

    # Une todos os meses em um único "caos" organizado
    df_total = pd.concat(lista_df, ignore_index=True)
    
    # Filtra pela OS e Quantidade
    resultado = df_total[(df_total['0S.'] == str(numero_os))]

    if not resultado.empty:
        # Retorna a primeira ocorrência encontrada, caso haja duplicatas
        return [resultado['DESCRIÇÃO DO SERVIÇO'].iloc[0], resultado['QUAT.'].iloc[0]]
    else:
        return "Serviço não encontrado para essa OS e Quantidade. Talvez ela tenha sido abduzida."

if __name__ == "__main__":
    lista_os_file = "/Users/mateusspinola/www/cadernos/lista_os.txt"

    if not os.path.exists(lista_os_file):
        print(f"Erro: Arquivo '{lista_os_file}' não encontrado.")
    else:
        with open(lista_os_file, "r") as f:
            for line in f:
                os_procurada = int(line.strip())
                result = buscar_descricao_servico(os_procurada)

                if isinstance(result, list):
                    descricao, quantidade = result
                else:
                    descricao = result
                    quantidade = "Não encontrado"

                print(f"OS: {os_procurada} | Qtd: {quantidade}")
                print(f"Descrição: {descricao}\n")