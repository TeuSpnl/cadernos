import os
import sys
import time
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
from webdriver_manager.chrome import ChromeDriverManager
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
from openpyxl.utils import get_column_letter
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
CONFIG_FILE = os.path.join(DRIVE_PATH, "TI compartilhado", "Financeiro", "[CONFIG] Dados Bancários Fornecedores.xlsx")
XFIN_URL = "https://app.xfin.com.br"
XFIN_USER = os.getenv('XFIN_USER')
XFIN_PASS = os.getenv('XFIN_PASS')

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
COL_XFIN_FORMA_PAGTO = "Forma Pagamento" # Precisa verificar o nome exato no CSV
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
    weekday = today.weekday() # 0=Seg, 5=Sab, 6=Dom
    
    start_date = today
    end_date = today

    if weekday == 5: # Sábado
        end_date = today + timedelta(days=2) # Até Segunda
    elif weekday == 6: # Domingo
        end_date = today + timedelta(days=1) # Até Segunda
    
    return start_date, end_date

def format_currency(value):
    if pd.isna(value): return "R$ 0,00"
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

# --- AUTOMATIZAÇÃO WEB (SELENIUM) ---

def download_xfin_report(status_callback):
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

    try:
        # 1. Login
        status_callback("Acessando Xfin...")
        driver.get(XFIN_URL)
        
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "Input_Email")))
        driver.find_element(By.ID, "Input_Email").send_keys(XFIN_USER)
        driver.find_element(By.ID, "Input_Password").send_keys(XFIN_PASS)
        
        btn_login = driver.find_element(By.CSS_SELECTOR, "button[type='submit'], input[type='submit']")
        btn_login.click()

        # Esperar login e redirecionamento (pode ter tela de escolha de módulo)
        WebDriverWait(driver, 20).until(lambda d: "Login" not in d.current_url)
        
        if "EscolheModulo" in driver.current_url:
            status_callback("Selecionando módulo Financeiro...")
            btn_financeiro = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[formaction*='ControleFinanceiro']"))
            )
            btn_financeiro.click()

        # 2. Navegar para Relatório de Contas a Pagar
        status_callback("Navegando para Contas a Pagar...")
        # URL direta para o relatório de contas a pagar (Ajuste se necessário)
        URL_RELATORIO = f"{XFIN_URL}/Relatorio/ContasAPagar" 
        driver.get(URL_RELATORIO)
        
        # 3. Filtros
        status_callback("Aplicando filtros de data...")
        
        # Datas: Hoje até Hoje + 15 dias
        dt_ini = date.today().strftime("%d/%m/%Y")
        dt_fim = (date.today() + timedelta(days=15)).strftime("%d/%m/%Y")
        
        # Tenta encontrar campos de data (IDs hipotéticos, ajustar conforme HTML real do Xfin)
        # Geralmente inputs do tipo date ou text com classe datepicker
        try:
            # Exemplo genérico, precisa inspecionar o Xfin real para IDs corretos
            # Supondo IDs 'dtInicial' e 'dtFinal'
            inp_ini = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "DataInicial")))
            inp_fim = driver.find_element(By.NAME, "DataFinal")
            
            inp_ini.clear()
            inp_ini.send_keys(dt_ini)
            inp_fim.clear()
            inp_fim.send_keys(dt_fim)
            
            # Botão Filtrar/Pesquisar
            btn_filtrar = driver.find_element(By.CSS_SELECTOR, "button[type='submit'], .btn-primary")
            btn_filtrar.click()
            
            time.sleep(3) # Espera carregar grid
        except Exception as e:
            print(f"Erro ao preencher filtros (pode ser necessário ajustar seletores): {e}")

        # 4. Exportar CSV
        status_callback("Baixando CSV...")
        # Procurar botão de exportar (Excel ou CSV)
        try:
            # Tenta achar botão com texto "Exportar" ou ícone
            btn_export = driver.find_element(By.XPATH, "//button[contains(text(), 'Exportar')] | //a[contains(text(), 'Exportar')] | //a[contains(@href, 'Exportar')]")
            btn_export.click()
            
            # Se abrir menu dropdown, selecionar CSV
            try:
                link_csv = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'CSV')]")))
                link_csv.click()
            except:
                pass # Talvez o botão já baixe direto
                
        except Exception as e:
            raise Exception(f"Botão de exportar não encontrado: {e}")

        # Esperar download
        time.sleep(5)
        
        # Verificar se baixou
        files = os.listdir(TEMP_DIR)
        csv_files = [f for f in files if f.endswith('.csv')]
        
        if not csv_files:
            raise Exception("Download do CSV falhou ou timeout.")
            
        return os.path.join(TEMP_DIR, csv_files[0])

    finally:
        driver.quit()

# --- PROCESSAMENTO DE DADOS ---

def process_data(csv_path, status_callback):
    status_callback("Lendo dados...")
    
    # 1. Ler CSV do Xfin
    # Tenta diferentes encodings e separadores
    try:
        df_xfin = pd.read_csv(csv_path, sep=';', encoding='utf-8-sig', dtype=str)
    except:
        df_xfin = pd.read_csv(csv_path, sep=',', encoding='latin1', dtype=str)
    
    # Normalizar colunas
    df_xfin.columns = [c.strip() for c in df_xfin.columns]
    
    # Mapeamento de colunas (Ajuste fino necessário com CSV real)
    # Procura colunas que contenham palavras chave se os nomes exatos não baterem
    def find_col(keywords):
        for col in df_xfin.columns:
            if any(k.lower() in col.lower() for k in keywords):
                return col
        return None

    col_fornecedor = find_col(['pessoa', 'fornecedor'])
    col_vencimento = find_col(['vencimento', 'data venc'])
    col_valor = find_col(['valor', 'valor liquido'])
    col_doc = find_col(['documento', 'doc', 'nota'])
    col_obs = find_col(['descri', 'obs'])
    col_forma = find_col(['forma', 'tipo doc']) # Ex: "5 - PIX"
    col_banco = find_col(['conta', 'banco'])    # Ex: "Banco do Brasil Peças"

    if not (col_fornecedor and col_vencimento and col_valor):
        raise Exception("Colunas essenciais não encontradas no CSV do Xfin.")

    # Filtrar pelo range de datas desejado (Operacional)
    start_date, end_date = get_date_range()
    
    # Converter vencimento para datetime
    df_xfin[col_vencimento] = pd.to_datetime(df_xfin[col_vencimento], dayfirst=True, errors='coerce')
    
    # Filtro de data
    mask = (df_xfin[col_vencimento].dt.date >= start_date) & (df_xfin[col_vencimento].dt.date <= end_date)
    df_filtered = df_xfin.loc[mask].copy()
    
    if df_filtered.empty:
        return None, [], start_date, end_date

    # 2. Ler Configuração Bancária
    status_callback("Lendo dados bancários...")
    if not os.path.exists(CONFIG_FILE):
        status_callback("Criando arquivo de configuração padrão...")
        create_default_config(CONFIG_FILE)

    df_config = pd.read_excel(CONFIG_FILE, dtype=str)
    # Normalizar nomes para merge (uppercase, strip)
    df_config['Fornecedor_Norm'] = df_config['Fornecedor'].str.upper().str.strip()
    df_filtered['Fornecedor_Norm'] = df_filtered[col_fornecedor].str.upper().str.strip()

    # 3. Buscar CNPJ no Firebird (Enriquecimento)
    status_callback("Consultando Firebird...")
    conn_fb = get_firebird_connection()
    fb_data = {}
    if conn_fb:
        cursor = conn_fb.cursor()
        # Busca todos fornecedores para criar um dict de lookup
        cursor.execute("SELECT NOME, CPF_CNPJ FROM FORNECEDOR")
        for row in cursor.fetchall():
            nome = row[0].strip().upper() if row[0] else ""
            cnpj = row[1].strip() if row[1] else ""
            fb_data[nome] = cnpj
        conn_fb.close()
    
    # Aplicar CNPJ do Firebird no DataFrame
    df_filtered['CNPJ_FB'] = df_filtered['Fornecedor_Norm'].map(fb_data)

    # 4. Merge com Configuração Bancária
    status_callback("Cruzando dados...")
    df_merged = pd.merge(df_filtered, df_config, on='Fornecedor_Norm', how='left')

    # Identificar faltantes
    missing_suppliers = df_merged[df_merged['Chave PIX'].isna() & df_merged['Banco'].isna()]['Fornecedor_Norm'].unique()

    # Preparar dados para Excel
    # Converter valor para float
    df_merged[col_valor] = df_merged[col_valor].astype(str).str.replace('R$', '').str.replace('.', '').str.replace(',', '.').astype(float)
    
    return df_merged, missing_suppliers, start_date, end_date, (col_fornecedor, col_doc, col_vencimento, col_obs, col_forma, col_banco)

# --- GERAÇÃO DE EXCEL ---

def create_excel(df, output_path, cols_map):
    col_forn, col_venc, col_valor, col_doc, col_obs, col_forma, col_banco = cols_map
    
    wb = openpyxl.Workbook()
    
    # Remover aba padrão
    if 'Sheet' in wb.sheetnames:
        wb.remove(wb['Sheet'])

    # Estilos
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
    
    # --- ABA PIX ---
    # Filtrar onde forma de pagamento contém "PIX" ou a preferência do config é PIX
    mask_pix = (df[col_forma].str.contains('PIX', case=False, na=False)) | (df['Forma Preferencial'].str.contains('PIX', case=False, na=False))
    df_pix = df[mask_pix].copy()
    
    if not df_pix.empty:
        ws_pix = wb.create_sheet("PAGAMENTOS PIX")
        headers = ["Nome Recebedor", "Nº Doc", "Vencimento", "Fornecedor", "Observação", "Chave PIX", "Valor", "Valor Total"]
        
        # Cabeçalho
        for col_num, header in enumerate(headers, 1):
            cell = ws_pix.cell(row=1, column=col_num, value=header)
            cell.font = header_font
            cell.fill = header_fill
            cell.border = border
            ws_pix.column_dimensions[get_column_letter(col_num)].width = 20

        # Dados
        df_pix = df_pix.sort_values(by=[col_forn]) # Ordenar para agrupar
        
        current_row = 2
        start_merge_row = 2
        current_supplier = None
        group_total = 0.0

        for idx, row in df_pix.iterrows():
            supplier = row[col_forn]
            val = row[col_valor]
            
            # Se mudou de fornecedor, finaliza o grupo anterior
            if supplier != current_supplier:
                if current_supplier is not None:
                    # Escreve total e mescla
                    if start_merge_row < current_row - 1:
                        ws_pix.merge_cells(start_row=start_merge_row, start_column=8, end_row=current_row-1, end_column=8)
                    
                    cell_total = ws_pix.cell(row=start_merge_row, column=8, value=group_total)
                    cell_total.number_format = '#,##0.00'
                    cell_total.alignment = Alignment(vertical='center')
                
                current_supplier = supplier
                start_merge_row = current_row
                group_total = 0.0

            group_total += val
            
            # Preenche linha
            ws_pix.cell(row=current_row, column=1, value=row.get('Nome Titular', '')) # Do config
            ws_pix.cell(row=current_row, column=2, value=row[col_doc])
            ws_pix.cell(row=current_row, column=3, value=row[col_venc].strftime('%d/%m/%Y'))
            ws_pix.cell(row=current_row, column=4, value=supplier)
            ws_pix.cell(row=current_row, column=5, value=row[col_obs])
            ws_pix.cell(row=current_row, column=6, value=row.get('Chave PIX', ''))
            c_val = ws_pix.cell(row=current_row, column=7, value=val)
            c_val.number_format = '#,##0.00'
            
            current_row += 1

        # Finaliza último grupo
        if current_supplier is not None:
            if start_merge_row < current_row - 1:
                ws_pix.merge_cells(start_row=start_merge_row, start_column=8, end_row=current_row-1, end_column=8)
            cell_total = ws_pix.cell(row=start_merge_row, column=8, value=group_total)
            cell_total.number_format = '#,##0.00'
            cell_total.alignment = Alignment(vertical='center')

    # --- ABAS POR BANCO/TIPO ---
    # Restante dos dados (não PIX)
    df_others = df[~mask_pix].copy()
    
    # Agrupar por Banco de Pagamento (coluna do Xfin) e Tipo Doc
    # Se a coluna de banco estiver vazia, usa "Indefinido"
    df_others['GroupKey'] = df_others[col_banco].fillna('Geral') + " - " + df_others[col_forma].fillna('Outros')
    
    groups = df_others.groupby('GroupKey')
    
    for name, group in groups:
        # Limpar nome da aba (max 31 chars, sem caracteres inválidos)
        sheet_name = name.replace('/', '-').replace('*', '')[:30]
        ws = wb.create_sheet(sheet_name)
        
        headers = ["Banco Recebedor", "Nº Doc", "Vencimento", "Fornecedor", "Observação", "CNPJ", "Valor"]
        for col_num, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col_num, value=header)
            cell.font = header_font
            cell.fill = header_fill
            
        r = 2
        for idx, row in group.iterrows():
            ws.cell(row=r, column=1, value=row.get('Banco', '')) # Do config
            ws.cell(row=r, column=2, value=row[col_doc])
            ws.cell(row=r, column=3, value=row[col_venc].strftime('%d/%m/%Y'))
            ws.cell(row=r, column=4, value=row[col_forn])
            ws.cell(row=r, column=5, value=row[col_obs])
            ws.cell(row=r, column=6, value=row.get('CNPJ_FB', '')) # Do Firebird
            c_val = ws.cell(row=r, column=7, value=row[col_valor])
            c_val.number_format = '#,##0.00'
            r += 1
            
    # Salvar
    wb.save(output_path)

# --- CLASSE PRINCIPAL DA UI ---

class PaymentBotApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Robô de Pagamentos Xfin")
        self.root.geometry("400x250")
        
        self.lbl_status = tk.Label(root, text="Pronto para iniciar", wraplength=350)
        self.lbl_status.pack(pady=20)
        
        self.progress = ttk.Progressbar(root, orient="horizontal", length=300, mode="indeterminate")
        self.progress.pack(pady=10)
        
        self.btn_start = tk.Button(root, text="Iniciar Extração", command=self.start_thread, height=2, width=20, bg="#4CAF50", fg="white")
        self.btn_start.pack(pady=20)

    def update_status(self, text):
        self.lbl_status.config(text=text)
        self.root.update_idletasks()

    def start_thread(self):
        self.btn_start.config(state="disabled")
        self.progress.start(10)
        threading.Thread(target=self.run_pipeline, daemon=True).start()

    def run_pipeline(self):
        try:
            self.update_status("Verificando ambiente...")
            check_drive_access()
            
            # Etapa A: Selenium
            csv_file = download_xfin_report(self.update_status)
            
            # Etapa B: Processamento
            df_final, missing, dt_start, dt_end, cols_map = process_data(csv_file, self.update_status)
            
            if df_final is None:
                self.finish("Nenhum pagamento encontrado para o período.")
                return

            # Etapa C: Salvar Arquivo
            self.update_status("Gerando planilha Excel...")
            
            # Estrutura de pastas: CONTAS A PAGAR\{ANO}\{Nº MÊS}. {NOME MÊS}\{DD-MM-AA}
            # Usando a data de INÍCIO do range (data operacional)
            year = dt_start.strftime("%Y")
            month_num = dt_start.strftime("%m")
            # Nome do mês em PT-BR (simples)
            months = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
            month_name = months[int(month_num)-1]
            folder_month = f"{month_num}. {month_name.upper()}"
            day_folder = dt_start.strftime("%d-%m-%y")
            
            base_path = os.path.join(DRIVE_PATH, "CONTAS A PAGAR", year, folder_month, day_folder)
            
            if not os.path.exists(base_path):
                os.makedirs(base_path)
                
            # Nome do arquivo
            if dt_start == dt_end:
                fname = f"Contas_A_Pagar-{dt_start.strftime('%d_%m_%Y')}-teste.xlsx"
            else:
                fname = f"Contas_A_Pagar-{dt_start.strftime('%d_%m')}-{dt_end.strftime('%d_%m_%Y')}-teste.xlsx"
                
            full_path = os.path.join(base_path, fname)
            
            create_excel(df_final, full_path, cols_map)
            
            # Alerta de Faltantes
            if len(missing) > 0 and email_alert:
                self.update_status("Enviando alerta de fornecedores...")
                # Cria um CSV temporário com os faltantes para anexar
                missing_df = pd.DataFrame(missing, columns=['Fornecedor'])
                missing_csv = os.path.join(TEMP_DIR, "falta_cadastrar.csv")
                missing_df.to_csv(missing_csv, index=False)
                email_alert.enviar_email_erro(missing_csv, len(missing))

            self.finish(f"Sucesso!\nAtualizado contas dos dias {dt_start.strftime('%d/%m')} a {dt_end.strftime('%d/%m')}\nSalvo em: {day_folder}")

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
        self.update_status(message)
        if error:
            messagebox.showerror("Erro", message)
        else:
            messagebox.showinfo("Concluído", message)

if __name__ == "__main__":
    root = tk.Tk()
    app = PaymentBotApp(root)
    root.mainloop()
