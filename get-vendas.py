import tkinter as tk
from tkinter import messagebox
import firebirdsql
import pandas as pd
import calendar
from datetime import datetime, timedelta
from tkcalendar import DateEntry

#########################
# CONFIGURAÇÕES GERAIS  #
#########################

HOST = '100.64.1.10'
PORT = 3050
DATABASE = r'C:\Micromais\mmSeculos\BD\SECULOS.MMDB'
USER = 'USER_CONSULTA'
PASSWORD = 'consultaseculos'
ROLE = 'apenasconsulta'
CHARSET = 'ISO8859_1'

# Função principal que faz a consulta e gera o Excel


def fetch_data(start_date, end_date):
    """
    start_date e end_date devem ser strings no formato 'YYYY-MM-DD'.
    Exemplo: '2024-12-01'.
    """
    try:
        # Conexão com o banco de dados Firebird
        conn = firebirdsql.connect(
            host=HOST,
            port=PORT,
            database=DATABASE,
            user=USER,
            password=PASSWORD,
            role=ROLE,
            auth_plugin_name='Legacy_Auth',
            wire_crypt=False,
            charset=CHARSET
        )

        print("Conexão bem-sucedida ao banco de dados!")

        cursor = conn.cursor()

        # Monta o WHERE com as datas
        # Ex: AND p.DATA BETWEEN '2024-12-01' AND '2024-12-15'
        where_dates = f"""
            p.DATA BETWEEN '{start_date}' AND '{end_date}'
        """

        # Total diário de vendas para Jucilande e Josuilton (exemplo original)
        total_query = f'''
            SELECT
                p.DATA,
                f.NOME,
                SUM(p.VALORCDESC) AS TOTAL_VENDAS
            FROM
                PEDIDOVENDA p
            INNER JOIN
                FUNCIONARIO f
            ON
                p.CDFUNC = f.CDFUNC
            INNER JOIN
                CLIENTE c
            ON
                p.CDCLIENTE = c.CDCLIENTE
            WHERE
                p.EFETIVADO = 'S'
                AND {where_dates}
                AND UPPER(c.NOME) NOT LIKE '%COMAGRO%'
            GROUP BY
                p.DATA, f.NOME
            ORDER BY
                p.DATA, f.NOME;
        '''

        cursor.execute(total_query)
        total_results = cursor.fetchall()

        # Criar DataFrame para o total diário
        total_df = pd.DataFrame(total_results, columns=["Data", "Vendedor", "Total Vendas"])

        # Média diária de vendas por vendedor
        avg_query = f'''
            SELECT
                DIARIO.NOME,
                AVG(DIARIO.TOTAL_VENDAS) AS MEDIA_DIARIA
            FROM (
                SELECT
                    p.DATA,
                    f.NOME AS NOME,
                    SUM(p.VALORCDESC) AS TOTAL_VENDAS
                FROM
                    PEDIDOVENDA p
                INNER JOIN
                    FUNCIONARIO f
                ON
                    p.CDFUNC = f.CDFUNC
                INNER JOIN
                    CLIENTE c
                ON
                    p.CDCLIENTE = c.CDCLIENTE
                WHERE
                    p.EFETIVADO = 'S'
                    AND {where_dates}
                    AND UPPER(c.NOME) NOT LIKE '%COMAGRO%'
                GROUP BY
                    p.DATA, f.NOME
            ) DIARIO
            GROUP BY
                DIARIO.NOME
            ORDER BY
                DIARIO.NOME;
        '''

        cursor.execute(avg_query)
        avg_results = cursor.fetchall()

        # Criar DataFrame para a média diária
        avg_df = pd.DataFrame(avg_results, columns=["Vendedor", "Média Diária"])

        # Convertendo valores para float (se necessário)
        if not total_df.empty:
            total_df["Total Vendas"] = total_df["Total Vendas"].astype(float)
        if not avg_df.empty:
            avg_df["Média Diária"] = avg_df["Média Diária"].astype(float)

        # Salvar resultados em uma planilha Excel
        with pd.ExcelWriter("arquivos/vendas_vendedores.xlsx") as writer:
            total_df.to_excel(writer, sheet_name="Total Diário", index=False)
            avg_df.to_excel(writer, sheet_name="Média Diária", index=False)

        print("Planilha Excel criada com sucesso: vendas_vendedores.xlsx")
        cursor.close()
        conn.close()

        # Exibir mensagem de sucesso ao usuário
        messagebox.showinfo("Sucesso", "Consulta concluída!\nPlanilha gerada em arquivos/vendas_vendedores.xlsx")

    except firebirdsql.Error as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        messagebox.showerror("Erro", f"Erro ao conectar ao banco de dados: {e}")

############################
# FUNÇÕES DE MANIPULAÇÃO   #
#         DE DATAS         #
############################


def get_current_month_range():
    """
    Retorna o primeiro dia e o dia atual (YYYY-MM-DD) do mês corrente.
    """
    today = datetime.today()
    start = datetime(today.year, today.month, 1).date()  # 1º dia do mês atual
    end = today.date()  # dia de hoje
    return start, end


def get_last_month_range():
    """
    Retorna (primeiro dia, último dia) do mês passado, no formato (YYYY-MM-DD).
    """
    today = datetime.today()
    # Se hoje é janeiro, mês passado é dezembro do ano anterior
    if today.month == 1:
        year_last_month = today.year - 1
        month_last_month = 12
    else:
        year_last_month = today.year
        month_last_month = today.month - 1

    start = datetime(year_last_month, month_last_month, 1).date()
    # Pegar o último dia do mês passado
    last_day = calendar.monthrange(year_last_month, month_last_month)[1]
    end = datetime(year_last_month, month_last_month, last_day).date()
    return start, end


def get_current_week_range():
    """
    Retorna (domingo passado ou hoje se domingo, hoje) da semana atual.
    Considerando domingo como início da semana.
    """
    today = datetime.today()
    # isoweekday(): segunda = 1, ..., domingo = 7
    # Para achar o domingo, subtraímos (isoweekday() % 7) dias.
    offset = today.isoweekday() % 7  # domingo = 7 -> offset = 0
    start = (today - timedelta(days=offset)).date()  # esse é o domingo
    end = today.date()
    return start, end


def get_last_week_range():
    """
    Retorna (domingo da semana passada, sábado da semana passada).
    Exemplo: se hoje é 2025-01-03 (sexta), 
      - domingo desta semana = 2024-12-29
      - domingo da semana passada = 2024-12-22
      - sábado da semana passada = 2024-12-28
    """
    today = datetime.today()
    offset = today.isoweekday() % 7  # domingo (7) -> offset=0
    # domingo desta semana:
    this_week_sunday = today - timedelta(days=offset)
    # domingo da semana passada:
    last_week_sunday = this_week_sunday - timedelta(weeks=1)
    # sábado da semana passada é um dia antes do domingo desta semana
    last_week_saturday = this_week_sunday - timedelta(days=1)
    return last_week_sunday.date(), last_week_saturday.date()

######################
# INTERFACE COM TK   #
######################


def generate_report():
    """
    Lê as datas em DD/MM/YYYY do DateEntry, converte para YYYY-MM-DD, 
    chama fetch_data.
    """
    start_str = start_date_var.get()
    end_str = end_date_var.get()

    # Converte de dd/mm/yyyy para date (datetime).
    try:
        start_date_obj = datetime.strptime(start_str, "%d/%m/%Y").date()
        end_date_obj = datetime.strptime(end_str, "%d/%m/%Y").date()
    except ValueError:
        messagebox.showerror("Erro", "Formato de data inválido. Use DD/MM/YYYY.")
        return

    # Verificação simples se data inicial é maior que data final
    if start_date_obj > end_date_obj:
        messagebox.showerror("Erro", "A data inicial não pode ser maior que a data final.")
        return

    # Converte de date para string em formato YYYY-MM-DD
    start_db = start_date_obj.strftime("%Y-%m-%d")
    end_db = end_date_obj.strftime("%Y-%m-%d")

    # Chama a função de gerar relatório
    fetch_data(start_db, end_db)


def fill_current_month():
    start, end = get_current_month_range()
    start_date_var.set(start.strftime("%d/%m/%Y"))
    end_date_var.set(end.strftime("%d/%m/%Y"))


def fill_last_month():
    start, end = get_last_month_range()
    start_date_var.set(start.strftime("%d/%m/%Y"))
    end_date_var.set(end.strftime("%d/%m/%Y"))


def fill_current_week():
    start, end = get_current_week_range()
    start_date_var.set(start.strftime("%d/%m/%Y"))
    end_date_var.set(end.strftime("%d/%m/%Y"))


def fill_last_week():
    start, end = get_last_week_range()
    start_date_var.set(start.strftime("%d/%m/%Y"))
    end_date_var.set(end.strftime("%d/%m/%Y"))


# Cria a janela principal
root = tk.Tk()
root.title("Relatório de Vendas - Firebird")

# Variáveis Tkinter para guardar as datas
start_date_var = tk.StringVar()
end_date_var = tk.StringVar()

# Frame para inputs
frame_inputs = tk.Frame(root)
frame_inputs.pack(padx=10, pady=10, fill=tk.X)


# Data inicial
tk.Label(frame_inputs, text="Data Inicial (DD/MM/YYYY):").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)

# Usando DateEntry do tkcalendar, que permite digitação OU calendário
start_date_entry = DateEntry(
    frame_inputs,
    textvariable=start_date_var,
    date_pattern='dd/MM/yyyy',  # Define o formato de exibição
    locale='pt_BR'              # Ajuste para exibir em português se preferir
)
start_date_entry.grid(row=0, column=1, padx=5, pady=5)

# Data final
tk.Label(frame_inputs, text="Data Final (DD/MM/YYYY):").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)

end_date_entry = DateEntry(
    frame_inputs,
    textvariable=end_date_var,
    date_pattern='dd/MM/yyyy',
    locale='pt_BR'
)
end_date_entry.grid(row=1, column=1, padx=5, pady=5)

# Frame para botões de período
frame_buttons = tk.Frame(root)
frame_buttons.pack(padx=10, pady=10, fill=tk.X)

btn_current_month = tk.Button(frame_buttons, text="Mês Atual", command=fill_current_month)
btn_current_month.pack(side=tk.LEFT, padx=5)

btn_current_week = tk.Button(frame_buttons, text="Semana Atual", command=fill_current_week)
btn_current_week.pack(side=tk.LEFT, padx=5)

btn_last_month = tk.Button(frame_buttons, text="Mês Passado", command=fill_last_month)
btn_last_month.pack(side=tk.LEFT, padx=5)

btn_last_week = tk.Button(frame_buttons, text="Semana Passada", command=fill_last_week)
btn_last_week.pack(side=tk.LEFT, padx=5)

# Botão para gerar o relatório
btn_generate = tk.Button(root, text="Gerar Relatório", command=generate_report)
btn_generate.pack(pady=10)

# Ajusta tamanho mínimo da janela
root.minsize(400, 200)

# Inicia o loop da aplicação
root.mainloop()