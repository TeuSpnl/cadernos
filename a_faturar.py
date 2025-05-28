import os
import firebirdsql
import pandas as pd
import numpy as np
import xlsxwriter
import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from tkinter import Tk
from tkinter.filedialog import askopenfilename

# Carregar variáveis de ambiente
load_dotenv()


def get_firebird_connection():
    # Conecta ao banco de dados Firebird
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


def choose_file():
    # Abre a caixa de diálogo para escolher o arquivo
    Tk().withdraw()  # Evita que a janela principal do Tkinter apareça
    filename = askopenfilename(title="Selecione o arquivo de entrada")
    return filename


def agrupar_pedidos(grupo):
    """
    Agrupa os pedidos de um cliente em grupos onde a diferença entre
    a data do primeiro pedido e os pedidos seguintes seja de até 7 dias.
    """
    grupos = []
    grupo_atual = []
    data_inicio = None
    
    for idx, row in grupo.iterrows():
        data_atual = row['DATA']
        if data_inicio is None:
            data_inicio = data_atual
            grupo_atual.append(idx)
        else:
            # Se a diferença entre a data atual e a data de início for <= 7 dias, adiciona ao grupo
            if (data_atual - data_inicio).days <= 10:
                grupo_atual.append(idx)
            else:
                grupos.append(grupo_atual)
                grupo_atual = [idx]
                data_inicio = data_atual
    if grupo_atual:
        grupos.append(grupo_atual)
    return grupos

filename = choose_file()

if not filename:
    print("Nenhum arquivo selecionado. Saindo...")
    exit()

# Lê a planilha a partir da linha 8 (ignorando as 7 primeiras linhas)
df = pd.read_excel(filename, skiprows=7)

# Converte a coluna DATA para datetime, considerando o formato dd/mm/yy (dayfirst=True)
df['DATA'] = pd.to_datetime(df['DATA'], dayfirst=True)

# Ordena por CLIENTE e DATA
df = df.sort_values(['CLIENTE', 'DATA'])

# Remove colunas indesejadas, se existirem
df = df.drop(columns=["VALOR", "DT ULTIMO PGTO", "PAGO"], errors='ignore')
 
# Muda o nome da coluna "FALTA" para "VALOR"
df.rename(columns={"FALTA": "VALOR"}, inplace=True)

# Remove linhas de clientes que contenham "comagro" no nome (case insensitive)
df = df[~df['CLIENTE'].str.contains("comagro", case=False, na=False)]

# Define a lista de clientes a serem removidos (a comparação é case insensitive)
remove_clients = []
df = df[~df['CLIENTE'].str.lower().isin([cliente.lower() for cliente in remove_clients])]

# Cria a coluna VENCIMENTO para armazenar a data de faturamento (como datetime)
df['VENCIMENTO'] = pd.NaT
df['MÉDIA'] = ''

# Para cada cliente, agrupa os pedidos e calcula a data média, somando 1 mês
for cliente, grupo in df.groupby('CLIENTE'):
    indices_grupo = agrupar_pedidos(grupo)
    for subgrupo in indices_grupo:
        datas = df.loc[subgrupo, 'DATA']
        # Converte as datas para números (em nanosegundos) e depois para dias
        medias_timestamp = datas.astype(np.int64)
        medias_dias = medias_timestamp / 86400e9  # conversão: ns para dias
        media_dias = medias_dias.mean()
        # Converte a média de volta para datetime (origem 1970-01-01)
        data_media = pd.to_datetime(media_dias, unit='D', origin='1970-01-01')
        # Adiciona 1 mês à data média para definir a data de faturamento
        data_fatura = data_media + relativedelta(months=1)
        df.loc[subgrupo, 'VENCIMENTO'] = data_fatura

# --- Parte 2: Consulta ao banco de dados para definir o TIPO FATUR. ---

# Define a data limite: 6 meses atrás a partir de hoje
data_limite = (datetime.datetime.today() - relativedelta(months=6)).date()
data_limite = data_limite.strftime("%Y-%m-%d")

# Cria um dicionário para mapear COD CLIENTE com o status de faturamento ('FATURAR' se houver recebimento Brasil)
client_fatur_status = {}

# Obtém os códigos de cliente únicos da planilha (coluna "COD CLIENTE")
client_codes = df['COD CLIENTE'].unique()

# Abre a conexão com o banco de dados
conn = get_firebird_connection()
cursor = conn.cursor()

# Para cada código de cliente, verifica se existe pelo menos um registro na tabela RECEBIMENTO
# onde CDFORMAPAG = 11 (Recebimento Brasil) e a data do recebimento (DTREC) é maior ou igual à data_limite.
sql = """
    SELECT 1 FROM RECEBIMENTO 
    WHERE CDCLIENTE = ? 
      AND CDFORMAPAG = 11 
      AND DATA >= ?
    ROWS 1
"""

for client in client_codes[:-1]:
    cursor.execute(sql, (int(client), data_limite))
    result = cursor.fetchone()
    if result:
        client_fatur_status[client] = 'FATURAR'
    else:
        client_fatur_status[client] = ''

cursor.close()
conn.close()

# Mapeia o status para cada linha da planilha, baseado na coluna "COD CLIENTE"
df['TIPO FATUR.'] = df['COD CLIENTE'].map(client_fatur_status)

# Reordena as colunas, colocando "MÉDIA" entre "VENCIMENTO" e "TIPO FATUR."
cols = list(df.columns)
if "VENCIMENTO" in cols and "MÉDIA" in cols and "TIPO FATUR." in cols:
    cols.remove("MÉDIA")
    pos_venc = cols.index("VENCIMENTO")
    cols.insert(pos_venc + 1, "MÉDIA")
    df = df[cols]

# --- Parte 3: Salvar a planilha com as datas no formato dd/mm/yy no Excel ---

save_filename = filename.replace('.xlsx', '_atualizado.xlsx')

from xlsxwriter.utility import xl_col_to_name

def add_media_formula(writer, df, sheet_name):
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]
    
    # Obtenha os índices dinâmicos das colunas relevantes
    media_col_idx = df.columns.get_loc("MÉDIA")
    data_col_idx = df.columns.get_loc("DATA")
    venc_col_idx = df.columns.get_loc("VENCIMENTO")
    
    # Converte os índices para letras de coluna no formato Excel
    data_col_letter = xl_col_to_name(data_col_idx)
    venc_col_letter = xl_col_to_name(venc_col_idx)
    
    total_rows = df.shape[0]
    
    for row_idx in range(total_rows):
        # Considera que o cabeçalho está na linha 0; os dados começam na linha 1 (Excel: linha 2)
        excel_row = row_idx + 2
        # Monta a fórmula: se o valor na coluna VENCIMENTO for maior que 0, subtrai o valor da coluna DATA; senão, deixa em branco.
        formula = f'=IF({venc_col_letter}{excel_row}>0,{venc_col_letter}{excel_row}-{data_col_letter}{excel_row},"")'
        worksheet.write_formula(excel_row - 1, media_col_idx, formula)
        

with pd.ExcelWriter(
        save_filename,
        engine='xlsxwriter',
        date_format='dd/mm/yy',
        datetime_format='dd/mm/yy') as writer:
    # Grava o DataFrame
    df.to_excel(writer, index=False)

    # Referências ao workbook/worksheet
    workbook  = writer.book
    worksheet = writer.sheets[df.columns.name or 'Sheet1']

    # --- Formatação de moeda na coluna VALOR ---
    # Índice da coluna VALOR
    valor_col_idx = df.columns.get_loc("VALOR")
    # Cria um formato de moeda em R$
    currency_fmt = workbook.add_format({'num_format': 'R$ #,##0.00'})
    # Define a largura (ex.: 15) e aplica o formato na coluna
    worksheet.set_column(valor_col_idx, valor_col_idx, 15, currency_fmt)

    # Adiciona as fórmulas da coluna MÉDIA
    add_media_formula(writer, df, df.columns.name or 'Sheet1')
