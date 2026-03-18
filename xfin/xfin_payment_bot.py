import os
import sys
import time
import threading
import tkinter as tk
import requests
from tkinter import messagebox, ttk
from datetime import date, datetime, timedelta
from tkcalendar import DateEntry
from dotenv import load_dotenv


# --- CONFIGURAÇÕES ---
DRIVE_PATH = r"\\100.64.1.10\Users\pichau\Documents\Drive Comagro"

# Carrega variáveis de ambiente do caminho de rede especificado
# ENV_PATH = r"\\100.64.1.10\Ti Compartilhado\Financeiro\.env"
ENV_PATH = os.path.join(DRIVE_PATH, "Ti Compartilhado", "Financeiro", ".env")
load_dotenv(ENV_PATH)

TEMP_DIR = os.path.join(os.getcwd(), "temp_xfin")
CONFIG_FILE = os.path.join(DRIVE_PATH, "CONTAS A PAGAR", "0 Extras", "[CONFIG] Dados Bancários Fornecedores.xlsx")
XFIN_URL = "https://app.xfin.com.br"
TK_XFIN = os.getenv('TK_XFIN')

# Configurações do Banco de Dados (Firebird)
FB_HOST = os.getenv('HOST')
FB_PORT = int(os.getenv('PORT', '3050'))
FB_DB = os.getenv('DB_PATH')
FB_USER = os.getenv('APP_USER')
FB_PASS = os.getenv('PASSWORD')
FB_ROLE = os.getenv('ROLE')
FB_AUTH = os.getenv('AUTH')

# Mapeamento de Colunas do Xfin (Resposta da API)
COL_XFIN_FORNECEDOR = "pessoa"
COL_XFIN_VENCIMENTO = "dataVencimento"
COL_XFIN_VALOR = "valor"
COL_XFIN_DOC = "numeroDocumento"
COL_XFIN_OBS = "descricao"
COL_XFIN_FORMA_PAGTO = "tipoDocumento"
COL_XFIN_BANCO_PAGAR = "banco"
COL_XFIN_FILIAL = "filial"

# --- FUNÇÕES AUXILIARES ---


def get_firebird_connection():
    import firebirdsql
    try:
        return firebirdsql.connect(
            host=FB_HOST,
            port=FB_PORT,
            database=FB_DB,
            user=FB_USER,
            password=FB_PASS,
            role=FB_ROLE,
            auth_plugin_name=FB_AUTH,
            wire_crypt=False,
            charset='ISO8859_1'
        )
    except Exception as e:
        print(f"Erro ao conectar ao Firebird: {e}")
        return None


def check_drive_access():
    if not os.path.exists(DRIVE_PATH):
        raise Exception(f"Drive de rede inacessível: {DRIVE_PATH}")
    return True


def get_date_range():
    """
    Calcula o intervalo de datas para o arquivo de saída.
    Se hoje for Sábado -> Sábado, Domingo, Segunda.
    Se hoje for Domingo -> Domingo, Segunda.
    Caso contrário -> Hoje.
    """
    today = date.today()
    weekday = today.weekday()  # 0=Seg, 5=Sab, 6=Dom

    start_date = today
    end_date = today

    if weekday == 5:  # Sábado
        end_date = today + timedelta(days=2)  # Até Segunda
    elif weekday == 6:  # Domingo
        end_date = today + timedelta(days=1)  # Até Segunda

    return start_date, end_date


def format_currency(value):
    import pandas as pd
    if pd.isna(value):
        return "R$ 0,00"
    try:
        return f"R$ {float(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return str(value)


def create_default_config(path):
    import pandas as pd
    """Cria o arquivo de configuração padrão com as colunas necessárias."""
    try:
        # Garante que o diretório existe
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # Colunas necessárias para o funcionamento do robô
        columns = [
            "Fornecedor",
            "CNPJ",
            "Chave PIX",
            "Banco",
            "Nome Titular"
        ]

        df = pd.DataFrame(columns=columns)
        df.to_excel(path, index=False)
        print(f"Arquivo de configuração criado em: {path}")
    except Exception as e:
        raise Exception(f"Erro ao criar arquivo de configuração padrão: {e}")


def identify_branch_group(val):
    """Identifica o grupo da filial baseado no CNPJ ou Nome."""
    val = str(val).upper()

    # 1. Verificação por CNPJ (Mais preciso)
    if "62.188.494" in val:
        return "Servicos"
    if "59.185.879" in val:
        return "Divisa"
    if "14.255.350" in val:
        return "Comagro"  # Loja (0001) e Oficina (0004)

    # 2. Verificação por Nome
    if "DIVISA" in val:
        return "Divisa"

    # Verificar PEÇAS ou OFICINA antes de SERVIÇOS para evitar ambiguidade
    if "PEÇAS" in val or "PECAS" in val or "OFICINA" in val:
        return "Comagro"

    if "SERVI" in val and "COMAGRO" in val:
        return "Servicos"
    if "COMAGRO" in val:
        return "Comagro"

    return "Geral"


def get_file_date(dt):
    import pandas as pd
    """Agrupa datas de fim de semana para a segunda-feira."""
    if pd.isna(dt):
        return date.today()
    if isinstance(dt, pd.Timestamp):
        dt = dt.date()

    weekday = dt.weekday()
    if weekday == 5:  # Sábado -> Segunda
        return dt + timedelta(days=2)
    if weekday == 6:  # Domingo -> Segunda
        return dt + timedelta(days=1)
    return dt

# --- INTEGRAÇÃO API XFIN ---

def fetch_xfin_data_api(status_callback, dt_ini, dt_fim, stop_event):
    import pandas as pd

    if not TK_XFIN:
        raise Exception("Token XFIN (TK_XFIN) não encontrado no arquivo .env. Configure-o para acessar a API.")

    status_callback("Iniciando busca na API...")

    # A API exige o formato yyyy-MM-dd
    dt_ini_api = datetime.strptime(dt_ini, "%d/%m/%Y").strftime("%Y-%m-%d")
    dt_fim_api = datetime.strptime(dt_fim, "%d/%m/%Y").strftime("%Y-%m-%d")

    url = f"{XFIN_URL}/api/v1/contasPagar"
    headers = {
        "accept": "*/*",
        "Authorization": f"Bearer {TK_XFIN}"
    }

    tamanho_pagina = 500
    pagina = 1
    todos_itens = []

    while True:
        if stop_event.is_set():
            return pd.DataFrame()

        status_callback(f"Buscando dados na API... Página {pagina}")
        params = {
            "pagina": pagina,
            "tamanhoPagina": tamanho_pagina,
            "dataVencimentoInicial": dt_ini_api,
            "dataVencimentoFinal": dt_fim_api
        }

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            if not data.get("sucesso", False):
                raise Exception(f"Erro na API: {data.get('mensagem', 'Desconhecido')}")

            itens = data.get("itens", [])
            todos_itens.extend(itens)

            total_paginas = data.get("totalPaginas", 1)
            if pagina >= total_paginas:
                break
            
            pagina += 1

        except Exception as e:
            print(f"Erro ao acessar API na página {pagina}: {e}")
            raise Exception(f"Falha na comunicação com a API XFIN: {e}")

    return pd.DataFrame(todos_itens)

# --- PROCESSAMENTO DE DADOS ---


def process_data(df_xfin, status_callback, stop_event):
    import pandas as pd
    import re
    status_callback("Processando dados da API...")

    if df_xfin is None or df_xfin.empty:
        return None, [], date.today(), date.today(), None

    col_fornecedor = COL_XFIN_FORNECEDOR
    col_vencimento = COL_XFIN_VENCIMENTO
    col_valor = COL_XFIN_VALOR
    col_doc = COL_XFIN_DOC
    col_obs = COL_XFIN_OBS
    col_forma = COL_XFIN_FORMA_PAGTO
    col_banco = COL_XFIN_BANCO_PAGAR
    col_filial = COL_XFIN_FILIAL

    # Converter vencimento para datetime
    df_xfin[col_vencimento] = pd.to_datetime(df_xfin[col_vencimento], dayfirst=True, errors='coerce')

    df_filtered = df_xfin.copy()

    # Determinar range de datas baseado nos dados para nomear o arquivo
    if not df_filtered.empty:
        start_date = df_filtered[col_vencimento].min().date()
        end_date = df_filtered[col_vencimento].max().date()
    else:
        start_date = date.today()
        end_date = date.today()

    if df_filtered.empty:
        return None, [], start_date, end_date

    # 2. Ler Configuração Bancária
    status_callback("Lendo dados bancários...")
    if not os.path.exists(CONFIG_FILE):
        status_callback("Criando arquivo de configuração padrão...")
        create_default_config(CONFIG_FILE)

    df_config = pd.read_excel(CONFIG_FILE, dtype=str)
    # Renomear colunas do config para evitar colisão com o CSV do Xfin
    df_config.columns = [f"Config_{c}" if c != "Fornecedor" else c for c in df_config.columns]

    # Normalizar nomes para merge (uppercase, strip)
    df_config['Fornecedor_Norm'] = df_config['Fornecedor'].str.upper().str.strip()

    def clean_supplier_name(name):
        if pd.isna(name):
            return ""
        name = str(name).upper().strip()
        # Remove "[CODE] - " prefix using regex to handle hyphens in name correctly
        match = re.match(r'^(\d+)\s*-\s*(.*)', name)
        if match:
            return match.group(2).strip()
        return name

    df_filtered['Fornecedor_Norm'] = df_filtered[col_fornecedor].apply(clean_supplier_name)

    # 3. Buscar CNPJ no Firebird (Enriquecimento)
    status_callback("Consultando Firebird...")
    conn_fb = get_firebird_connection()
    if stop_event.is_set():
        if conn_fb:
            conn_fb.close()
        return None, [], start_date, end_date, None

    fb_data = {}
    if conn_fb:
        cursor = conn_fb.cursor()
        # Busca todos fornecedores para criar um dict de lookup
        cursor.execute("SELECT NOME, CPF_CNPJ FROM FORNECEDOR")
        for row in cursor.fetchall():
            nome = row[0].strip().upper() if row[0] else ""
            cnpj = row[1].strip() if row[1] else ""
            fb_data[nome] = cnpj
        print(f"Carregados {len(fb_data)} fornecedores do Firebird.")
        conn_fb.close()

    # Aplicar CNPJ do Firebird no DataFrame
    df_filtered['CNPJ_FB'] = df_filtered['Fornecedor_Norm'].map(fb_data)

    # 4. Merge com Configuração Bancária
    status_callback("Cruzando dados...")
    df_merged = pd.merge(df_filtered, df_config, on='Fornecedor_Norm', how='left')

    # Identificar faltantes
    missing_suppliers = df_merged[df_merged['Config_Chave PIX'].isna(
    ) & df_merged['Config_Banco'].isna()]['Fornecedor_Norm'].unique()

    # Preparar dados para Excel
    # A API já retorna valores numéricos, mas garantimos o tipo e tratamos vazios
    df_merged[col_valor] = pd.to_numeric(df_merged[col_valor], errors='coerce').fillna(0.0)

    # Identificar grupo da filial
    if col_filial:
        df_merged['Filial_Group'] = df_merged[col_filial].apply(identify_branch_group)
    else:
        df_merged['Filial_Group'] = 'Geral'

    # 5. Definir CNPJ Final (Prioridade: Excel > Firebird)
    if 'Config_CNPJ' in df_merged.columns:
        df_merged['CNPJ_Final'] = df_merged['Config_CNPJ'].fillna(df_merged['CNPJ_FB'])
    else:
        df_merged['CNPJ_Final'] = df_merged['CNPJ_FB']

    # 6. Extrair Fatura da Descrição (Feature Nova - Strict)
    def extract_invoice_strict(row):
        desc = str(row[col_obs]) if col_obs and pd.notna(row[col_obs]) else ""
        if " - " in desc:
            # Pega a última parte após o último hífen
            candidate = desc.rsplit(" - ", 1)[-1].strip()
            # Validação estrita: aceita apenas se for numérico puro (sem /, letras, pontos)
            if candidate.isdigit():
                return candidate
        return ""

    df_merged['Fatura'] = df_merged.apply(extract_invoice_strict, axis=1)

    return df_merged, missing_suppliers, start_date, end_date, (
        col_fornecedor, col_vencimento, col_valor, col_doc, col_obs, col_forma, col_banco)


def clean_sheet_name(name):
    """Remove caracteres inválidos para nome de aba do Excel."""
    invalid_chars = ['\\', '/', '*', '[', ']', ':', '?']
    for char in invalid_chars:
        name = name.replace(char, '')
    # Excel limita a 31 caracteres
    return name[:31]

# --- GERAÇÃO DE EXCEL ---


def create_excel(df, output_path, cols_map):
    import openpyxl
    import pandas as pd
    from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
    from openpyxl.utils import get_column_letter
    col_forn, col_venc, col_valor, col_doc, col_obs, col_forma, col_banco = cols_map

    wb = openpyxl.Workbook()

    # Remover aba padrão
    if 'Sheet' in wb.sheetnames:
        wb.remove(wb['Sheet'])

    # Preparar dados para o Resumo
    summary_data = []  # Lista de tuplas (Nome da Aba, Valor Total)

    # Garantir colunas de agrupamento
    if col_forma and col_forma in df.columns:
        df[col_forma] = df[col_forma].fillna('Indefinido')
    else:
        df['__Forma_Temp'] = 'Indefinido'
        col_forma = '__Forma_Temp'

    if col_banco and col_banco in df.columns:
        df[col_banco] = df[col_banco].fillna('')
    else:
        df['__Banco_Temp'] = ''
        col_banco = '__Banco_Temp'

    # Agrupar por Tipo de Documento (col_forma)
    # Ordenar para que NF/NOTA venha primeiro, BOLETO em seguida, depois o PIX e depois os demais em ordem alfabética
    doc_vals = list(df[col_forma].astype(str).fillna('').unique())

    def _doc_priority(x):
        xu = x.upper()
        if "NF" in xu:
            return (0, xu)
        if "BOLETO" in xu:
            return (1, xu)
        if "PIX" in xu:
            return (2, xu)
        return (3, xu)

    doc_types = sorted(doc_vals, key=_doc_priority)

    for doc_type in doc_types:
        df_doc = df[df[col_forma] == doc_type].copy()

        # Verificar se há múltiplos bancos para este tipo de documento
        unique_banks = df_doc[col_banco].unique()
        # Remove bancos vazios da contagem se houver outros
        real_banks = [b for b in unique_banks if b.strip()]

        # Lógica de separação de abas
        sub_groups = []
        if len(real_banks) > 1:
            # Separa por banco
            for bank in unique_banks:
                sub_df = df_doc[df_doc[col_banco] == bank]
                if sub_df.empty:
                    continue

                s_name = f"{doc_type}"
                if bank.strip():
                    s_name += f" - {bank}"
                sub_groups.append((s_name, sub_df))
        else:
            # Aba única
            sub_groups.append((doc_type, df_doc))

        # Criar abas
        for sheet_name, group_df in sub_groups:
            safe_name = clean_sheet_name(sheet_name)
            ws = wb.create_sheet(safe_name)

            # Formato de Moeda Brasileiro
            currency_fmt = '_-R$* #,##0.00_-;-R$* #,##0.00_-;_-R$* \"-\"??_-;_-@_-'

            # Definir Cores do Cabeçalho
            dt_upper = doc_type.upper()
            if "NF" in dt_upper or "NOTA" in dt_upper:
                fill_color = "FFFF00"  # Amarelo
                font_color = "000000"  # Preto
            elif "BOLETO" in dt_upper:
                fill_color = "366092"  # Azul (Padrão anterior)
                font_color = "FFFFFF"  # Branco
            elif "CRÉDITO" in dt_upper or "CREDITO" in dt_upper or "ESTORNO" in dt_upper:
                fill_color = "00B050"  # Verde
                font_color = "FFFFFF"
            elif "DÉBITO" in dt_upper or "DEBITO" in dt_upper:
                fill_color = "FF0000"  # Vermelho
                font_color = "FFFFFF"
            elif "PIX" in dt_upper:
                fill_color = "e56700"  # Laranja
                font_color = "FFFFFF"
            else:
                fill_color = "000000"  # Preto (Outros)
                font_color = "FFFFFF"

            header_font = Font(bold=True, color=font_color)
            header_fill = PatternFill(start_color=fill_color, end_color=fill_color, fill_type="solid")
            border = Border(left=Side(style='thin'), right=Side(style='thin'),
                            top=Side(style='thin'), bottom=Side(style='thin'))

            # Determinar Layout (PIX ou Padrão)
            is_pix_layout = "PIX" in doc_type.upper()

            if is_pix_layout:
                headers = ["Vencimento", "Nome Recebedor", "Fornecedor",
                           "Chave PIX", "Observação", "Valor", "Valor Total"]
                val_col_idx = 7
            else:
                headers = ["Vencimento", "Banco/Conta", "Fornecedor", "CNPJ", "Observação", "Valor"]
                val_col_idx = 6

            # Cabeçalho
            for col_num, header in enumerate(headers, 1):
                cell = ws.cell(row=1, column=col_num, value=header)
                cell.font = header_font
                cell.fill = header_fill
                cell.border = border
                ws.column_dimensions[get_column_letter(col_num)].width = 20

            # Dados
            if is_pix_layout:
                # Calcular total por fornecedor para ordenação (mantendo agrupamento)
                group_df['__Total_Supplier'] = group_df.groupby(col_forn)[col_valor].transform('sum')
                group_df = group_df.sort_values(
                    by=['__Total_Supplier', col_forn, col_valor],
                    ascending=[True, True, True])
            else:
                # Agrupar Faturas (se houver)
                if 'Fatura' in group_df.columns:
                    group_df['Fatura'] = group_df['Fatura'].fillna('').astype(str).str.strip()
                    mask_invoice = group_df['Fatura'] != ''
                    df_invoice = group_df[mask_invoice].copy()
                    df_no_invoice = group_df[~mask_invoice].copy()

                    if not df_invoice.empty:
                        grp_keys = ['Fatura', col_forn, col_venc]
                        if 'CNPJ_Final' in df_invoice.columns:
                            df_invoice['CNPJ_Final'] = df_invoice['CNPJ_Final'].fillna('')
                            grp_keys.append('CNPJ_Final')
                        if 'Config_Banco' in df_invoice.columns:
                            df_invoice['Config_Banco'] = df_invoice['Config_Banco'].fillna('')
                            grp_keys.append('Config_Banco')

                        grouped_rows = []
                        for key, block in df_invoice.groupby(grp_keys):
                            row_data = block.iloc[0].copy()
                            row_data[col_valor] = block[col_valor].sum()
                            if col_obs:
                                row_data[col_obs] = f"Fatura - {key[0]}"
                            grouped_rows.append(row_data)
                        group_df = pd.concat([df_no_invoice, pd.DataFrame(grouped_rows)], ignore_index=True)

                group_df = group_df.sort_values(by=[col_valor], ascending=True)

            current_row = 2
            sheet_total = 0.0

            # Variáveis para agrupamento PIX
            start_merge_row = 2
            current_supplier = None
            supplier_total = 0.0

            for idx, row in group_df.iterrows():
                val = row[col_valor]
                sheet_total += val

                if is_pix_layout:
                    supplier = row[col_forn]
                    if supplier != current_supplier:
                        if current_supplier is not None:
                            if start_merge_row < current_row - 1:
                                ws.merge_cells(start_row=start_merge_row, start_column=7,
                                               end_row=current_row-1, end_column=7)
                            ws.cell(row=start_merge_row, column=7, value=supplier_total).number_format = currency_fmt
                            ws.cell(row=start_merge_row, column=7).alignment = Alignment(vertical='center')
                        current_supplier = supplier
                        start_merge_row = current_row
                        supplier_total = 0.0
                    supplier_total += val

                    ws.cell(row=current_row, column=1, value=row[col_venc].strftime('%d/%m/%Y'))
                    ws.cell(row=current_row, column=2, value=row.get('Config_Nome Titular', ''))
                    ws.cell(row=current_row, column=3, value=supplier)
                    ws.cell(row=current_row, column=4, value=row.get('Config_Chave PIX', ''))
                    ws.cell(row=current_row, column=5, value=row[col_obs] if col_obs and col_obs in row else "")
                    ws.cell(row=current_row, column=6, value=val).number_format = currency_fmt
                else:
                    # Layout Padrão
                    banco_val = row.get('Config_Banco', '')
                    ws.cell(row=current_row, column=1, value=row[col_venc].strftime('%d/%m/%Y'))
                    ws.cell(row=current_row, column=2, value=banco_val)
                    ws.cell(row=current_row, column=3, value=row[col_forn])
                    ws.cell(row=current_row, column=4, value=row.get('CNPJ_Final', ''))
                    ws.cell(row=current_row, column=5, value=row[col_obs] if col_obs and col_obs in row else "")
                    ws.cell(row=current_row, column=6, value=val).number_format = currency_fmt

                current_row += 1

            # Finalizar último grupo PIX
            if is_pix_layout and current_supplier is not None:
                if start_merge_row < current_row - 1:
                    ws.merge_cells(start_row=start_merge_row, start_column=7, end_row=current_row-1, end_column=7)
                ws.cell(row=start_merge_row, column=7, value=supplier_total).number_format = currency_fmt
                ws.cell(row=start_merge_row, column=7).alignment = Alignment(vertical='center')

            # Linha de Total da Aba
            total_row = current_row + 1
            ws.cell(row=total_row, column=val_col_idx-1, value="TOTAL:").font = Font(bold=True)

            col_letter = get_column_letter(val_col_idx)
            sum_formula = f"=SUM({col_letter}2:{col_letter}{current_row-1})"
            c_total = ws.cell(row=total_row, column=val_col_idx, value=sum_formula)
            c_total.number_format = currency_fmt
            c_total.font = Font(bold=True)

            summary_data.append((ws.title, f"{col_letter}{total_row}"))

    # --- ABA TOTAIS ---
    if summary_data:
        ws = wb.create_sheet("Totais")  # Cria no final por padrão
        ws.column_dimensions['A'].width = 30
        ws.column_dimensions['B'].width = 20

        # Formato de Moeda
        currency_fmt = '_-R$* #,##0.00_-;-R$* #,##0.00_-;_-R$* \"-\"??_-;_-@_-'

        # Estilo Padrão para Totais
        header_font_tot = Font(bold=True, color="FFFFFF")
        header_fill_tot = PatternFill(start_color="366092", end_color="366092", fill_type="solid")

        # Cabeçalho
        cell_h1 = ws.cell(row=1, column=1, value="Tipo de Pagamento")
        cell_h2 = ws.cell(row=1, column=2, value="Valor Total")
        for c in [cell_h1, cell_h2]:
            c.font = header_font_tot
            c.fill = header_fill_tot
            c.border = border

        r = 2
        for name, cell_ref in summary_data:
            ws.cell(row=r, column=1, value=name)
            c_val = ws.cell(row=r, column=2, value=f"='{name}'!{cell_ref}")
            c_val.number_format = currency_fmt
            r += 1

        # Total Geral
        r += 1
        cell_gt_lbl = ws.cell(row=r, column=1, value="TOTAL GERAL")
        cell_gt_val = ws.cell(row=r, column=2, value=f"=SUM(B2:B{r-1})")

        for c in [cell_gt_lbl, cell_gt_val]:
            c.font = Font(bold=True, size=12)
            c.border = border
        cell_gt_val.number_format = currency_fmt

    # Salvar
    try:
        wb.save(output_path)
    except PermissionError:
        output_path = output_path + ".error"

    wb.save(output_path)

# --- CLASSE PRINCIPAL DA UI ---


class PaymentBotApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Robô de Pagamentos Xfin")
        self.root.geometry("450x300")

        self.stop_event = threading.Event()

        self.lbl_status = tk.Label(root, text="Pronto para iniciar", wraplength=350)
        self.lbl_status.pack(pady=20)

        # Inputs de Data
        frame_dates = tk.Frame(root)
        frame_dates.pack(pady=5)
        tk.Label(frame_dates, text="Início:").pack(side=tk.LEFT)
        self.entry_start = DateEntry(frame_dates, width=12, background='darkblue',
                                     foreground='white', borderwidth=2, locale='pt_BR', date_pattern='dd/mm/yyyy')
        self.entry_start.pack(side=tk.LEFT, padx=5)
        self.entry_start.set_date(date.today())

        tk.Label(frame_dates, text="Fim:").pack(side=tk.LEFT)
        self.entry_end = DateEntry(frame_dates, width=12, background='darkblue',
                                   foreground='white', borderwidth=2, locale='pt_BR', date_pattern='dd/mm/yyyy')
        self.entry_end.pack(side=tk.LEFT, padx=5)
        self.entry_end.set_date(date.today() + timedelta(days=15))

        # Botões de Data Rápida
        frame_quick_dates = tk.Frame(root)
        frame_quick_dates.pack(pady=5)

        tk.Button(frame_quick_dates, text="Hoje", command=lambda: self.set_dates(0)).pack(side=tk.LEFT, padx=2)
        tk.Button(frame_quick_dates, text="Amanhã", command=lambda: self.set_dates(
            1, start_today=False)).pack(side=tk.LEFT, padx=2)
        tk.Button(frame_quick_dates, text="3 Dias", command=lambda: self.set_dates(3)).pack(side=tk.LEFT, padx=2)
        tk.Button(frame_quick_dates, text="7 Dias", command=lambda: self.set_dates(7)).pack(side=tk.LEFT, padx=2)
        tk.Button(frame_quick_dates, text="15 Dias", command=lambda: self.set_dates(15)).pack(side=tk.LEFT, padx=2)

        self.var_merge_days = tk.BooleanVar()
        self.chk_merge = tk.Checkbutton(
            root, text="Fundir dias em um único arquivo (Feriados)", variable=self.var_merge_days)
        self.chk_merge.pack(pady=5)

        self.progress = ttk.Progressbar(root, orient="horizontal", length=300, mode="indeterminate")
        self.progress.pack(pady=10)

        frame_actions = tk.Frame(root)
        frame_actions.pack(pady=20)

        self.btn_start = tk.Button(frame_actions, text="Iniciar Extração", command=self.start_thread,
                                   height=2, width=15, bg="#4CAF50", fg="black")
        self.btn_start.pack(side=tk.LEFT, padx=10)

        self.btn_cancel = tk.Button(frame_actions, text="Cancelar", command=self.cancel_process,
                                    height=2, width=15, bg="#f44336", fg="black", state="disabled")
        self.btn_cancel.pack(side=tk.LEFT, padx=10)

    def set_dates(self, days, start_today=True):
        today = date.today()
        if start_today:
            self.entry_start.set_date(today)
            self.entry_end.set_date(today + timedelta(days=days))
        else:
            # Case for "Amanhã" where start is also tomorrow
            tomorrow = today + timedelta(days=1)
            self.entry_start.set_date(tomorrow)
            self.entry_end.set_date(tomorrow)

    def update_status(self, text):
        self.lbl_status.config(text=text)
        self.root.update_idletasks()

    def start_thread(self):
        self.stop_event.clear()
        self.btn_start.config(state="disabled")
        self.btn_cancel.config(state="normal")
        self.progress.start(10)
        threading.Thread(target=self.run_pipeline, daemon=True).start()

    def cancel_process(self):
        if not self.stop_event.is_set():
            self.update_status("Cancelando... Aguarde.")
            self.stop_event.set()

    def run_pipeline(self):
        import shutil
        import pandas as pd
        try:
            import email_alert
        except ImportError:
            email_alert = None

        try:
            dt_ini = self.entry_start.get()
            dt_fim = self.entry_end.get()

            self.update_status("Verificando ambiente...")
            check_drive_access()

            # Pasta necessária para o report de erros do email
            if not os.path.exists(TEMP_DIR):
                os.makedirs(TEMP_DIR)

            # Etapa A: Buscar na API
            if self.stop_event.is_set():
                return
            df_xfin = fetch_xfin_data_api(self.update_status, dt_ini, dt_fim, self.stop_event)

            if self.stop_event.is_set():
                self.finish("Processo cancelado pelo usuário.")
                return

            # Etapa B: Processamento
            df_merged, missing, dt_start, dt_end, cols_map = process_data(
                df_xfin, self.update_status, self.stop_event)

            if self.stop_event.is_set():
                self.finish("Processo cancelado pelo usuário.")
                return

            if df_merged is None or df_merged.empty:
                self.finish("Nenhum pagamento encontrado para o período.")
                return

            # Etapa C: Salvar Arquivos
            self.update_status("Gerando planilha Excel...")
            print("Gerando planilha Excel...")

            # Nome do mês em PT-BR (simples)
            months = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
                      "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]

            generated_files = []

            # Agrupar por Data de Arquivo (juntando FDS na Segunda)
            col_venc = cols_map[1]
            df_merged['File_Date'] = df_merged[col_venc].apply(get_file_date)

            print("Iniciando geração dos arquivos por data e filial...")

            merge_days = self.var_merge_days.get()

            if merge_days:
                # Lógica de Fusão: Pega a data mais distante (max) para a pasta
                max_file_date = df_merged['File_Date'].max()
                min_file_date = df_merged['File_Date'].min()

                year = max_file_date.strftime("%Y")
                month_num = max_file_date.month
                month_name = months[month_num-1]
                folder_month = f"{month_num}. {month_name.upper()}"
                day_folder = max_file_date.strftime("%d-%m-%y")

                current_base_path = os.path.join(DRIVE_PATH, "CONTAS A PAGAR", year, folder_month, day_folder)
                if not os.path.exists(current_base_path):
                    os.makedirs(current_base_path)

                date_str_initial = min_file_date.strftime('%d_%m_%Y')
                date_str_final = max_file_date.strftime('%d_%m_%Y')

                for group_name, df_group in df_merged.groupby('Filial_Group'):
                    print(f"Processando (Fundido): {date_str_initial} a {date_str_final} - Filial {group_name}")

                    suffix = f"_{group_name}" if group_name != "Geral" else ""
                    if date_str_initial == date_str_final:
                        fname = f"Contas_A_Pagar{suffix}-{date_str_final}.xlsx"
                    else:
                        fname = f"Contas_A_Pagar{suffix}-{date_str_initial}-{date_str_final}.xlsx"

                    full_path = os.path.join(current_base_path, fname)
                    create_excel(df_group, full_path, cols_map)
                    generated_files.append(fname)
            else:
                # Loop por Data (Comportamento Padrão)
                for file_date, df_date in df_merged.groupby('File_Date'):
                    if self.stop_event.is_set():
                        self.finish("Processo cancelado pelo usuário.")
                        return

                    # Estrutura de pastas: CONTAS A PAGAR\{ANO}\{Nº MÊS}. {NOME MÊS}\{DD-MM-AA}
                    year = file_date.strftime("%Y")
                    month_num = file_date.month  # Número sem zero à esquerda
                    month_name = months[month_num-1]
                    folder_month = f"{month_num}. {month_name.upper()}"
                    day_folder = file_date.strftime("%d-%m-%y")

                    current_base_path = os.path.join(DRIVE_PATH, "CONTAS A PAGAR", year, folder_month, day_folder)

                    if not os.path.exists(current_base_path):
                        os.makedirs(current_base_path)

                    date_str = file_date.strftime('%d_%m_%Y')

                    # Loop por Filial dentro da Data
                    for group_name, df_group in df_date.groupby('Filial_Group'):
                        print(f"Processando: Data {date_str} - Filial {group_name} ({len(df_group)} registros)")

                        if df_group.empty:
                            continue

                        # Nome do arquivo: Contas_A_Pagar_FILIAL-DD_MM_AAAA.xlsx
                        suffix = f"_{group_name}" if group_name != "Geral" else ""
                        fname = f"Contas_A_Pagar{suffix}-{date_str}.xlsx"

                        full_path = os.path.join(current_base_path, fname)
                        create_excel(df_group, full_path, cols_map)
                        generated_files.append(fname)

            print("Arquivos gerados com sucesso.")

            # Alerta de Faltantes
            if len(missing) > 0 and email_alert:
                if self.stop_event.is_set():
                    self.finish("Processo cancelado pelo usuário.")
                    return

                self.update_status("Enviando alerta de fornecedores...")
                # Cria um CSV temporário com os faltantes para anexar
                missing_df = pd.DataFrame(missing, columns=['Fornecedor'])
                missing_csv = os.path.join(TEMP_DIR, "falta_cadastrar.csv")
                missing_df.to_csv(missing_csv, index=False)
                email_alert.enviar_email_erro(missing_csv, len(missing), True)

            self.finish(f"Sucesso!\nGerados: {len(generated_files)} arquivos\nSalvos nas pastas de data.")

        except Exception as e:
            self.finish(f"Erro: {str(e)}", error=True)
        finally:
            # Limpeza
            if os.path.exists(TEMP_DIR):
                try:
                    shutil.rmtree(TEMP_DIR)
                except:
                    pass

    def finish(self, message, error=False):
        self.progress.stop()
        self.btn_start.config(state="normal")
        self.btn_cancel.config(state="disabled")
        self.update_status(message)
        if error:
            messagebox.showerror("Erro", message)
        else:
            messagebox.showinfo("Concluído", message)


if __name__ == "__main__":
    root = tk.Tk()
    app = PaymentBotApp(root)
    root.mainloop()
