# get-clientes_obs.py
import os
import re
import datetime
import firebirdsql
import pandas as pd
from queue import Queue
import concurrent.futures
from openpyxl import Workbook
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# ===================== Funções de Conexão =====================


def get_firebird_connection():
    # Ajuste os parâmetros conforme sua configuração, inclusive charset
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


conn = get_firebird_connection()
cursor = conn.cursor()

query = """
            SELECT NOME, SITUACAO, OBS
            FROM CLIENTE
            ORDER BY NOME
        """

cursor.execute(query)
rows = cursor.fetchall()


def sanitize_string(value):
    """
    Remove caracteres ilegais para XML (usado pelo OpenPyXL) de uma string.
    """
    # Essa expressão regular remove caracteres com códigos 0-8, 11, 12 e 14-31.
    illegal_xml_chars_re = re.compile(r'[\000-\010\013\014\016-\037]')
    return illegal_xml_chars_re.sub("", value)


wb = Workbook()
ws = wb.active

for cliente in rows:
    processed_row = []
    for column in cliente:
        # Se for do tipo bytes, decodifica-o para string
        if isinstance(column, bytes):
            try:
                decoded = column.decode("iso-8859-1", errors="replace")
            except UnicodeDecodeError:
                decoded = column.decode("utf-8", errors="replace")
            processed_row.append(sanitize_string(decoded))
        else:
            processed_row.append(column)
    # Adiciona a linha completa ao worksheet
    ws.append(processed_row)

# Salva o arquivo Excel
output_filename = "clientes-obs.xlsx"
wb.save(output_filename)
print(f"Arquivo Excel salvo como '{output_filename}'")
