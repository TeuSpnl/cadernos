import os
import sys
import time
import re
import shutil
import pandas as pd
import firebirdsql
import openpyxl
import threading
import tkinter as tk
from tkinter import messagebox, ttk
from datetime import date, datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from webdriver_manager.chrome import ChromeDriverManager
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
from openpyxl.utils import get_column_letter
from tkcalendar import DateEntry
from dotenv import load_dotenv

# Import local modules (assuming they are in the same directory)
try:
    import email_alert
except ImportError:
    print("Aviso: Módulo email_alert não encontrado. O envio de e-mails será desativado.")
    email_alert = None

load_dotenv()

# --- CONFIGURAÇÕES ---
DRIVE_PATH = r"\\100.64.1.10\Users\pichau\Documents\Drive Comagro"
TEMP_DIR = os.path.join(os.getcwd(), "temp_xfin")
CONFIG_FILE = os.path.join(DRIVE_PATH, "CONTAS A PAGAR", "0 Extras", "[CONFIG] Dados Bancários Fornecedores.xlsx")
XFIN_URL = "https://app.xfin.com.br"
XFIN_USER = os.getenv('XFIN_USER')
XFIN_PASS = os.getenv('XFIN_PASS')
URL_ESCOLHA_FILIAL = f"{XFIN_URL}/Identity/Account/EscolheFilial"

# Configurações do Banco de Dados (Firebird)
FB_HOST = os.getenv('HOST')
FB_PORT = int(os.getenv('PORT', '3050'))
FB_DB = os.getenv('DB_PATH')
FB_USER = os.getenv('APP_USER')
FB_PASS = os.getenv('PASSWORD')
FB_ROLE = os.getenv('ROLE')
FB_AUTH = os.getenv('AUTH')

# Mapeamento de Colunas do Xfin (Ajustar conforme o CSV real exportado)
# Assumindo nomes prováveis baseados no contexto
COL_XFIN_FORNECEDOR = "Pessoa"
COL_XFIN_VENCIMENTO = "Vencimento"
COL_XFIN_VALOR = "Valor"
COL_XFIN_DOC = "Número Documento"
COL_XFIN_OBS = "Descrição"
COL_XFIN_FORMA_PAGTO = "Forma Pagamento"  # Precisa verificar o nome exato no CSV
COL_XFIN_BANCO_PAGAR = "Conta/Banco"     # Precisa verificar o nome exato no CSV

# --- FUNÇÕES AUXILIARES ---


def get_firebird_connection():
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
    if pd.isna(value):
        return "R$ 0,00"
    try:
        return f"R$ {float(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return str(value)


def create_default_config(path):
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
            "Agência",
            "Conta",
            "Forma Preferencial",
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

# --- AUTOMATIZAÇÃO WEB (SELENIUM) ---


def login_xfin(driver, status_callback):
    status_callback("Realizando login...")
    if "Login" not in driver.current_url:
        driver.get(XFIN_URL)

    try:
        # Check if already logged in
        if "Login" not in driver.current_url and "EscolheModulo" not in driver.current_url and "EscolheFilial" not in driver.current_url:
            # Tenta verificar se tem algum elemento de login, se não tiver, assume logado
            if not driver.find_elements(By.ID, "Input_Email"):
                return True

        if driver.find_elements(By.ID, "Input_Email"):
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "Input_Email")))
            driver.find_element(By.ID, "Input_Email").send_keys(XFIN_USER)
            driver.find_element(By.ID, "Input_Password").send_keys(XFIN_PASS)

            btn_login = driver.find_element(By.CSS_SELECTOR, "button[type='submit'], input[type='submit']")
            btn_login.click()

            WebDriverWait(driver, 20).until(lambda d: "Login" not in d.current_url)

        if "EscolheModulo" in driver.current_url:
            status_callback("Selecionando módulo Financeiro...")
            btn_financeiro = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[formaction*='ControleFinanceiro']"))
            )
            btn_financeiro.click()

            time.sleep(1)

        if "EscolheFilial" in driver.current_url:
            # 2. Obtém lista de filiais
            filiais = get_branches(driver)

            status_callback("Selecionando filial padrão...")
            select_branch(driver, filiais[0]['id'])  # Seleciona a primeira filial como padrão

            time.sleep(.75)

        return True
    except Exception as e:
        print(f"Erro no login: {e}")
        return False


def get_branches(driver):
    try:
        driver.get(URL_ESCOLHA_FILIAL)

        select_elem = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "Input_IdFilial"))
        )

        select = Select(select_elem)
        branches = []

        for option in select.options:
            val = option.get_attribute("value")
            text = option.text
            if val:
                branches.append({"id": val, "nome": text})

        return branches
    except Exception as e:
        print(f"Erro ao obter filiais: {e}")
        return []


def select_branch(driver, branch_id):
    try:
        if "EscolheFilial" not in driver.current_url:
            driver.get(URL_ESCOLHA_FILIAL)

        select_elem = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "Input_IdFilial"))
        )
        select = Select(select_elem)
        select.select_by_value(branch_id)

        btn_escolher = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
        btn_escolher.click()

        WebDriverWait(driver, 10).until(lambda d: "EscolheFilial" not in d.current_url)
        return True
    except Exception as e:
        print(f"Erro ao selecionar filial {branch_id}: {e}")
        return False


def download_xfin_report(status_callback, dt_ini, dt_fim, stop_event):
    status_callback("Iniciando navegador...")

    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)
    os.makedirs(TEMP_DIR)

    options = webdriver.ChromeOptions()
    prefs = {"download.default_directory": TEMP_DIR}
    options.add_experimental_option("prefs", prefs)
    options.add_argument("--start-maximized")
    # options.add_argument("--headless") # Descomente para rodar sem interface gráfica

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    downloaded_files = []

    try:
        if not login_xfin(driver, status_callback):
            raise Exception("Falha ao realizar login.")

        if stop_event.is_set():
            return []

        # Obter lista de filiais via tela de escolha (mais robusto que o filtro da página)
        status_callback("Mapeando filiais...")
        branches = get_branches(driver)

        if not branches:
            raise Exception("Nenhuma filial encontrada.")

        for branch in branches:
            if stop_event.is_set():
                break

            status_callback(f"Processando: {branch['nome']}...")

            try:
                # 1. Mudar contexto da filial (Garante que o relatório abra na filial certa)
                if not select_branch(driver, branch['id']):
                    print(f"Pulo filial {branch['nome']} por erro de seleção.")
                    continue

                # 2. Navegar para o Relatório
                URL_RELATORIO = f"{XFIN_URL}/ContaPagar"
                driver.get(URL_RELATORIO)

                # 3. Preencher Datas (JS)
                driver.execute_script(f"$('#txtDataInicialVencimento').val('{dt_ini}');")
                driver.execute_script(f"$('#txtDataFinalVencimento').val('{dt_fim}');")

                # 4. Clicar em Buscar
                # O HTML mostra: onclick="BuscarTitulos(true)"
                btn_buscar = driver.find_element(By.XPATH, "//button[contains(text(), 'Buscar')]")
                driver.execute_script("arguments[0].click();", btn_buscar)

                # Aguarda o loading (geralmente o Xfin mostra um spinner ou bloqueia a tela)
                time.sleep(3)

                # 5. Limpar pasta temporária de arquivos antigos (ContasAPagar.csv) para evitar conflito
                for f in os.listdir(TEMP_DIR):
                    if f.startswith("ContasAPagar") and f.endswith(".csv"):
                        try:
                            os.remove(os.path.join(TEMP_DIR, f))
                        except:
                            pass

                # 6. Clicar em Exportar
                # O HTML mostra: onclick="ExportarTitulos()"
                btn_export = driver.find_element(By.XPATH, "//button[contains(text(), 'Exportar')]")
                driver.execute_script("arguments[0].click();", btn_export)

                # 7. Loop de Espera pelo Download
                # Espera até aparecer um arquivo novo que não seja .crdownload
                timeout = 5
                elapsed = 0
                downloaded_file = None

                while elapsed < timeout:
                    if stop_event.is_set():
                        break

                    files = os.listdir(TEMP_DIR)
                    # Procura o arquivo padrão do Xfin (geralmente ContasAPagar.csv)
                    candidates = [f for f in files if f.endswith('.csv') and not f.startswith('branch_')]

                    if candidates:
                        downloaded_file = candidates[0]
                        # Verifica se terminou de baixar (não tem .crdownload associado)
                        if not any(f.endswith('.crdownload') for f in files):
                            break

                    time.sleep(1)
                    elapsed += 1

                if downloaded_file:
                    original_path = os.path.join(TEMP_DIR, downloaded_file)
                    new_name = f"branch_{branch['id']}_{downloaded_file}"
                    new_path = os.path.join(TEMP_DIR, new_name)

                    # Remove se já existir (reprocessamento)
                    if os.path.exists(new_path):
                        os.remove(new_path)

                    os.rename(original_path, new_path)
                    downloaded_files.append(new_path)
                    print(f"Arquivo salvo: {new_name}")
                else:
                    print(f"Timeout ou erro ao baixar arquivo da filial {branch['nome']}")

            except Exception as e:
                print(f"Erro ao processar filial {branch['nome']}: {e}")
                continue

        return downloaded_files

    finally:
        driver.quit()

# --- PROCESSAMENTO DE DADOS ---


def process_data(csv_paths, status_callback, stop_event):
    status_callback("Lendo dados...")

    if not csv_paths:
        return None, [], date.today(), date.today(), None

    dfs = []
    for csv_path in csv_paths:
        try:
            # Tenta detectar a linha de cabeçalho (procura por 'Vencimento' ou 'Pessoa')
            header_row = 0
            try:
                with open(csv_path, 'r', encoding='latin1') as f:
                    for i, line in enumerate(f):
                        if i > 20:
                            break
                        if 'Vencimento' in line or 'Pessoa' in line:
                            header_row = i
                            break
            except:
                pass

            try:
                df = pd.read_csv(csv_path, sep=';', encoding='latin1', dtype=str,
                                 header=header_row, on_bad_lines='skip', engine='python')
            except:
                df = pd.read_csv(csv_path, sep=',', encoding='latin1', dtype=str,
                                 header=header_row, on_bad_lines='skip', engine='python')
            dfs.append(df)
        except Exception as e:
            print(f"Erro ao ler {csv_path}: {e}")

    if not dfs:
        raise Exception("Nenhum CSV válido lido.")

    df_xfin = pd.concat(dfs, ignore_index=True)

    # Normalizar colunas
    df_xfin.columns = [c.strip() for c in df_xfin.columns]

    # Mapeamento de colunas (Ajuste fino necessário com CSV real)
    # Procura colunas que contenham palavras chave se os nomes exatos não baterem
    def find_col(keywords, exclude=None):
        for col in df_xfin.columns:
            col_lower = col.lower()
            if any(k.lower() in col_lower for k in keywords):
                if exclude and any(e.lower() in col_lower for e in exclude):
                    continue
                return col
        return None

    col_fornecedor = find_col(['pessoa', 'fornecedor'])
    col_vencimento = find_col(['vencimento', 'data venc'])
    col_valor = find_col(['valor', 'valor liquido'])
    col_doc = find_col(['documento', 'doc', 'nota'])
    col_obs = find_col(['descri', 'obs'])
    col_forma = find_col(['forma', 'tipo de doc'])  # Ex: "5 - PIX"
    col_banco = find_col(['banco'], exclude=['plano'])    # Ex: "Banco do Brasil Peças"
    col_filial = find_col(['empresa', 'filial', 'unidade'])  # Coluna para separar filiais

    if not (col_fornecedor and col_vencimento and col_valor):
        raise Exception("Colunas essenciais não encontradas no CSV do Xfin.")

    # Converter vencimento para datetime
    df_xfin[col_vencimento] = pd.to_datetime(df_xfin[col_vencimento], dayfirst=True, errors='coerce')

    # NÃO Filtrar por data (solicitação do usuário: pegar todos os dias disponíveis no arquivo)
    # O filtro de data já foi feito no download do Xfin (15 dias)
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
        if conn_fb: conn_fb.close()
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
    # Converter valor para float
    df_merged[col_valor] = df_merged[col_valor].astype(str).str.replace(
        'R$', '').str.replace(
        '.', '').str.replace(
        ',', '.').astype(float)

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

    # 6. Extrair Fatura da Descrição (Feature Nova)
    def extract_invoice(row):
        desc = str(row[col_obs]) if col_obs and pd.notna(row[col_obs]) else ""
        if " - " in desc:
            # Pega a última parte após o último hífen
            return desc.rsplit(" - ", 1)[-1].strip()
        return ""
    
    df_merged['Fatura'] = df_merged.apply(extract_invoice, axis=1)

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
    doc_types = df[col_forma].unique()

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
                            if col_obs: row_data[col_obs] = f"Fatura - {key[0]}"
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
                                ws.merge_cells(start_row=start_merge_row, start_column=8,
                                               end_row=current_row-1, end_column=8)
                            ws.cell(row=start_merge_row, column=8, value=supplier_total).number_format = currency_fmt
                            ws.cell(row=start_merge_row, column=8).alignment = Alignment(vertical='center')
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
                    ws.merge_cells(start_row=start_merge_row, start_column=8, end_row=current_row-1, end_column=8)
                ws.cell(row=start_merge_row, column=8, value=supplier_total).number_format = currency_fmt
                ws.cell(row=start_merge_row, column=8).alignment = Alignment(vertical='center')

            # Linha de Total da Aba
            total_row = current_row + 1
            ws.cell(row=total_row, column=val_col_idx-1, value="TOTAL:").font = Font(bold=True)
            c_total = ws.cell(row=total_row, column=val_col_idx, value=sheet_total)
            c_total.number_format = currency_fmt
            c_total.font = Font(bold=True)

            summary_data.append((safe_name, sheet_total))

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
        grand_total = 0.0
        for name, total in summary_data:
            ws.cell(row=r, column=1, value=name)
            c_val = ws.cell(row=r, column=2, value=total)
            c_val.number_format = currency_fmt
            grand_total += total
            r += 1

        # Total Geral
        r += 1
        cell_gt_lbl = ws.cell(row=r, column=1, value="TOTAL GERAL")
        cell_gt_val = ws.cell(row=r, column=2, value=grand_total)

        for c in [cell_gt_lbl, cell_gt_val]:
            c.font = Font(bold=True, size=12)
            c.border = border
        cell_gt_val.number_format = currency_fmt

    # Salvar
    wb.save(output_path)

# --- CLASSE PRINCIPAL DA UI ---


class PaymentBotApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Robô de Pagamentos Xfin")
        self.root.geometry("450x400")
        
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
        tk.Button(frame_quick_dates, text="Amanhã", command=lambda: self.set_dates(1, start_today=False)).pack(side=tk.LEFT, padx=2)
        tk.Button(frame_quick_dates, text="3 Dias", command=lambda: self.set_dates(3)).pack(side=tk.LEFT, padx=2)
        tk.Button(frame_quick_dates, text="7 Dias", command=lambda: self.set_dates(7)).pack(side=tk.LEFT, padx=2)
        tk.Button(frame_quick_dates, text="15 Dias", command=lambda: self.set_dates(15)).pack(side=tk.LEFT, padx=2)

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
        try:
            dt_ini = self.entry_start.get()
            dt_fim = self.entry_end.get()

            self.update_status("Verificando ambiente...")
            check_drive_access()

            # Etapa A: Selenium
            if self.stop_event.is_set(): return
            csv_files = download_xfin_report(self.update_status, dt_ini, dt_fim, self.stop_event)
            
            if self.stop_event.is_set():
                self.finish("Processo cancelado pelo usuário.")
                return

            print(f"Arquivos CSV baixados: {csv_files}")

            # Etapa B: Processamento
            df_merged, missing, dt_start, dt_end, cols_map = process_data(csv_files, self.update_status, self.stop_event)
            
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

            # Loop por Data
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
