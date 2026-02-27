import os
import pandas as pd
import firebirdsql
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkcalendar import DateEntry
from dotenv import load_dotenv
from datetime import datetime

# Carregar variáveis de ambiente
load_dotenv()

class ConciliadorFinanceiro:
    def __init__(self, root):
        self.root = root
        self.root.title("Conciliação Financeira - Sistema vs Caixa")
        self.root.geometry("1000x700")

        # Variáveis de Estado
        self.df_resultado = pd.DataFrame()
        self.caminho_arquivo_excel = tk.StringVar()
        self.data_selecionada = tk.StringVar()

        # Configuração da Interface
        self._setup_ui()

    def _setup_ui(self):
        # --- Frame de Filtros ---
        frame_top = tk.LabelFrame(self.root, text="Filtros e Arquivos", padx=10, pady=10)
        frame_top.pack(fill="x", padx=10, pady=5)

        # Seletor de Data
        tk.Label(frame_top, text="Data da Conferência:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.cal_data = DateEntry(frame_top, width=12, background='darkblue',
                                  foreground='white', borderwidth=2, locale='pt_BR', date_pattern='dd/mm/yyyy')
        self.cal_data.grid(row=0, column=1, padx=5, pady=5, sticky="w")

        # Seletor de Arquivo Excel
        tk.Label(frame_top, text="Arquivo do Caixa (Excel):").grid(row=1, column=0, padx=5, pady=5, sticky="w")
        tk.Entry(frame_top, textvariable=self.caminho_arquivo_excel, width=50, state="readonly").grid(row=1, column=1, padx=5, pady=5)
        tk.Button(frame_top, text="Selecionar Arquivo", command=self.selecionar_arquivo).grid(row=1, column=2, padx=5, pady=5)

        # Botão Processar
        tk.Button(frame_top, text="PROCESSAR CONCILIAÇÃO", bg="#4CAF50", fg="black", font=("Arial", 10, "bold"),
                  command=self.processar_conciliacao).grid(row=2, column=0, columnspan=3, pady=15, sticky="we")

        # --- Frame de Resultados ---
        frame_results = tk.LabelFrame(self.root, text="Resultado da Conciliação", padx=10, pady=10)
        frame_results.pack(fill="both", expand=True, padx=10, pady=5)

        # Treeview
        columns = ("origem", "numero", "forma_pag", "valor", "status")
        self.tree = ttk.Treeview(frame_results, columns=columns, show="headings")
        
        # Cabeçalhos
        self.tree.heading("origem", text="Origem")
        self.tree.heading("numero", text="Número Doc.")
        self.tree.heading("forma_pag", text="Forma Pagamento")
        self.tree.heading("valor", text="Valor (R$)")
        self.tree.heading("status", text="Status")

        # Larguras
        self.tree.column("origem", width=100, anchor="center")
        self.tree.column("numero", width=100, anchor="center")
        self.tree.column("forma_pag", width=200, anchor="w")
        self.tree.column("valor", width=100, anchor="e")
        self.tree.column("status", width=150, anchor="center")

        # Scrollbar
        scrollbar = ttk.Scrollbar(frame_results, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscroll=scrollbar.set)
        self.tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # Tags de Cores
        self.tree.tag_configure("ok", background="#C8E6C9")       # Verde Claro
        self.tree.tag_configure("missing", background="#FFCDD2")  # Vermelho Claro (Faltando no Caixa)
        self.tree.tag_configure("extra", background="#FFF9C4")    # Amarelo Claro (Não no Sistema)

        # --- Frame Rodapé ---
        frame_footer = tk.Frame(self.root, padx=10, pady=10)
        frame_footer.pack(fill="x")

        tk.Button(frame_footer, text="Exportar Resultado para Excel", command=self.exportar_excel).pack(side="right")

    # --- Funções de Conexão e Dados ---
    @staticmethod
    def get_firebird_connection():
        # Ajustar com os parâmetros corretos do Firebird, inclusive charset
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

    def buscar_dados_banco(self, data_str):
        """Busca dados de PEDIDOVENDA, ORDEMSERVICO e RECEBIMENTO."""
        conn = self.get_firebird_connection()
        cursor = conn.cursor()

        # Formatar data para YYYY-MM-DD para o banco
        data_obj = datetime.strptime(data_str, '%d/%m/%Y')
        data_db = data_obj.strftime('%Y-%m-%d')

        # Queries unificadas logicamente no Python
        # 1. Pedido Venda
        sql_pv = """
            SELECT 'PEDIDO' as TIPO, P.CDPEDIDOVENDA as NUMERO, P.VALORCDESC, F.DESCRICAO
            FROM PEDIDOVENDA P
            JOIN FORMAPAG F ON P.CDFORMAPAG = F.CDFORMAPAG
            WHERE P.DATA = ? AND P.EFETIVADO = 'S'
        """
        
        # 2. Ordem Serviço
        sql_os = """
            SELECT 'OS' as TIPO, O.CDORDEMSERVICO as NUMERO, O.VALORCDESC, F.DESCRICAO
            FROM ORDEMSERVICO O
            JOIN FORMAPAG F ON O.CDFORMAPAG = F.CDFORMAPAG
            WHERE O.DATA = ? AND O.EFETIVADO = 'S'
        """

        # 3. Recebimento (Avulso/Dívida)
        sql_rec = """
            SELECT 'RECEB' as TIPO, R.CDRECEBIMENTO as NUMERO, R.VALORTOTAL, F.DESCRICAO
            FROM RECEBIMENTO R
            JOIN FORMAPAG F ON R.CDFORMAPAG = F.CDFORMAPAG
            WHERE R.DATA = ? AND R.PEDIDO = 'N'
        """

        dfs = []
        try:
            # Executar Pedido Venda
            cursor.execute(sql_pv, (data_db,))
            rows = cursor.fetchall()
            if rows:
                dfs.append(pd.DataFrame(rows, columns=['ORIGEM', 'NUMERO', 'VALOR', 'FORMA_PAG']))

            # Executar OS
            cursor.execute(sql_os, (data_db,))
            rows = cursor.fetchall()
            if rows:
                dfs.append(pd.DataFrame(rows, columns=['ORIGEM', 'NUMERO', 'VALOR', 'FORMA_PAG']))

            # Executar Recebimento
            cursor.execute(sql_rec, (data_db,))
            rows = cursor.fetchall()
            if rows:
                dfs.append(pd.DataFrame(rows, columns=['ORIGEM', 'NUMERO', 'VALOR', 'FORMA_PAG']))

        except Exception as e:
            raise Exception(f"Erro ao consultar banco de dados: {e}")
        finally:
            conn.close()

        if not dfs:
            return pd.DataFrame(columns=['ORIGEM', 'NUMERO', 'VALOR', 'FORMA_PAG'])
        
        return pd.concat(dfs, ignore_index=True)

    def selecionar_arquivo(self):
        filename = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx *.xls")])
        if filename:
            self.caminho_arquivo_excel.set(filename)

    def processar_conciliacao(self):
        data_str = self.cal_data.get()
        arquivo_excel = self.caminho_arquivo_excel.get()

        if not arquivo_excel:
            messagebox.showwarning("Aviso", "Selecione o arquivo Excel do caixa.")
            return

        try:
            # 1. Buscar dados do Sistema (DB)
            df_sistema = self.buscar_dados_banco(data_str)
            
            # Normalizar dados do sistema
            df_sistema['VALOR'] = pd.to_numeric(df_sistema['VALOR'], errors='coerce').fillna(0)
            df_sistema['FORMA_PAG'] = df_sistema['FORMA_PAG'].astype(str).str.strip().str.upper()
            df_sistema['FONTE'] = 'SISTEMA'

            # 2. Buscar dados do Caixa (Excel)
            df_caixa = pd.read_excel(arquivo_excel)
            
            # Validação básica de colunas
            cols_necessarias = ['Nº PEDIDO', 'FORMA PAG.', 'VALOR C/ DESC.']
            if not all(col in df_caixa.columns for col in cols_necessarias):
                messagebox.showerror("Erro Excel", f"O Excel deve conter as colunas: {cols_necessarias}")
                return

            # Normalizar dados do caixa
            df_caixa = df_caixa[cols_necessarias].copy()
            df_caixa.rename(columns={
                'Nº PEDIDO': 'NUMERO',
                'FORMA PAG.': 'FORMA_PAG',
                'VALOR C/ DESC.': 'VALOR'
            }, inplace=True)
            df_caixa['VALOR'] = pd.to_numeric(df_caixa['VALOR'], errors='coerce').fillna(0)
            df_caixa['FORMA_PAG'] = df_caixa['FORMA_PAG'].astype(str).str.strip().str.upper()
            df_caixa['ORIGEM'] = 'CAIXA'
            df_caixa['FONTE'] = 'CAIXA'

            # 3. Lógica de Cruzamento (Cumcount para duplicatas)
            # Criar ID único para cada transação baseada em (FormaPag + Valor)
            # Isso permite diferenciar dois pagamentos de R$ 50,00 em Dinheiro no mesmo dia
            
            # Contador para Sistema
            df_sistema['match_id'] = df_sistema.groupby(['FORMA_PAG', 'VALOR']).cumcount()
            
            # Contador para Caixa
            df_caixa['match_id'] = df_caixa.groupby(['FORMA_PAG', 'VALOR']).cumcount()

            # Merge Outer (Full Join)
            # Chave de junção: Forma Pagamento, Valor e o ID sequencial (match_id)
            df_merge = pd.merge(
                df_sistema, 
                df_caixa, 
                on=['FORMA_PAG', 'VALOR', 'match_id'], 
                how='outer', 
                suffixes=('_SIS', '_CX'),
                indicator=True
            )

            # 4. Classificação dos Resultados
            def classificar_status(row):
                if row['_merge'] == 'both':
                    return 'OK'
                elif row['_merge'] == 'left_only':
                    return 'FALTANDO NO CAIXA' # Está no sistema, não no caixa
                elif row['_merge'] == 'right_only':
                    return 'NÃO NO SISTEMA'    # Está no caixa, não no sistema
                return 'ERRO'

            df_merge['STATUS'] = df_merge.apply(classificar_status, axis=1)

            # Limpeza final para exibição
            df_merge['NUMERO'] = df_merge['NUMERO_SIS'].fillna(df_merge['NUMERO_CX'])
            df_merge['ORIGEM_FINAL'] = df_merge['ORIGEM_SIS'].fillna('CAIXA_AVULSO')
            
            # Selecionar colunas finais
            self.df_resultado = df_merge[['ORIGEM_FINAL', 'NUMERO', 'FORMA_PAG', 'VALOR', 'STATUS']].copy()
            
            # Ordenar pelo número do pedido (convertendo para numérico para garantir ordem correta)
            self.df_resultado['NUMERO_SORT'] = pd.to_numeric(self.df_resultado['NUMERO'], errors='coerce')
            self.df_resultado.sort_values(by=['NUMERO_SORT', 'ORIGEM_FINAL'], inplace=True)
            self.df_resultado.drop(columns=['NUMERO_SORT'], inplace=True)
            
            self.atualizar_treeview()
            messagebox.showinfo("Sucesso", "Conciliação processada com sucesso!")

        except firebirdsql.OperationalError as e:
            messagebox.showerror("Erro de Conexão", f"Não foi possível conectar ao Firebird.\nVerifique VPN/Rede.\nErro: {e}")
        except Exception as e:
            messagebox.showerror("Erro", f"Ocorreu um erro inesperado:\n{e}")

    def atualizar_treeview(self):
        # Limpar tabela atual
        for i in self.tree.get_children():
            self.tree.delete(i)

        if self.df_resultado.empty:
            return

        for _, row in self.df_resultado.iterrows():
            status = row['STATUS']
            valor_fmt = f"{row['VALOR']:.2f}"
            
            tag = ""
            if status == 'OK':
                tag = "ok"
            elif status == 'FALTANDO NO CAIXA':
                tag = "missing" # Vermelho
            elif status == 'NÃO NO SISTEMA':
                tag = "extra"   # Amarelo

            values = (row['ORIGEM_FINAL'], row['NUMERO'], row['FORMA_PAG'], valor_fmt, status)
            self.tree.insert("", "end", values=values, tags=(tag,))

    def exportar_excel(self):
        if self.df_resultado.empty:
            messagebox.showwarning("Aviso", "Não há dados para exportar. Processe primeiro.")
            return

        filename = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")])
        if filename:
            try:
                self.df_resultado.to_excel(filename, index=False)
                messagebox.showinfo("Sucesso", f"Arquivo salvo em:\n{filename}")
            except Exception as e:
                messagebox.showerror("Erro", f"Erro ao salvar arquivo: {e}")

if __name__ == "__main__":
    root = tk.Tk()
    app = ConciliadorFinanceiro(root)
    root.mainloop()
