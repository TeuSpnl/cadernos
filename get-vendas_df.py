import re
import pdfplumber
import pandas as pd
import tkinter as tk
from tkinter import filedialog

# Função para converter strings numéricas (formatação brasileira: ponto para milhar e vírgula para decimal)
def parse_num(s):
    s = s.replace('.', '').replace(',', '.')
    try:
        return float(s)
    except:
        return None

# Lista de possíveis nomes de vendedor – ordenados do maior para o menor (para garantir que "DANIEL HENRIQUE SANTOS BEZERRA" seja detectado antes de "JU")
vendedores_conhecidos = ["DANIEL HENRIQUE SANTOS BEZERRA", "PALMIRO", "JU"]

# Abre a caixa de diálogo para escolher o arquivo PDF
root = tk.Tk()
root.withdraw()
file_path = filedialog.askopenfilename(title="Selecione o arquivo PDF", filetypes=[("PDF files", "*.pdf")])
if not file_path:
    raise Exception("Nenhum arquivo foi selecionado.")

# Nova expressão regular:
# Estrutura assumida (a partir do texto extraído):
# "COMAGRO {Data} {Pedido} {NotaFiscal (10 dígitos, opcional)} {Cliente+Vendedor} {Vendedor2}-{CondiçãoPagto} {ValorProduto} {ValorFinal} {TotalIPI} {Markup} {%Atendimento}"
pattern = re.compile(
    r'^COMAGRO\s+'
    r'(\d{2}/\d{2}/\d{2})\s+'      # Grupo 1: Data
    r'(Z\d+)\s+'                  # Grupo 2: Pedido
    r'(\d{10})?\s*'               # Grupo 3: Nota Fiscal (opcional)
    r'(.+?)\s+'                   # Grupo 4: Campo combinado de Cliente + Vendedor
    r'(\d{2})-(.+?)\s+'           # Grupo 5: Código Vendedor 2; Grupo 6: Condição de Pagto (parte textual)
    r'([\d\.,]+)\s+'             # Grupo 7: Valor Produto
    r'([\d\.,]+)\s+'             # Grupo 8: Valor Final
    r'([\d\.,]+)\s+'             # Grupo 9: Total IPI
    r'([\d\.,]+)\s+'             # Grupo 10: Markup
    r'([\d\.,]+)$'               # Grupo 11: % Atendimento
)

# Lista para armazenar os dados
rows = []

with pdfplumber.open(file_path) as pdf:
    for page in pdf.pages:
        text = page.extract_text()
        if not text:
            continue
        for line in text.split('\n'):
            match = pattern.match(line)
            if match:
                # Campo combinado (grupo 4) que contém "Cliente" e "Vendedor" juntos
                campo_combinado = match.group(4).strip()
                # Inicialmente, assume-se que o vendedor não foi detectado
                vendedor_extraido = ""
                cliente_extraido = campo_combinado
                # Verifica se o campo termina com algum dos nomes de vendedor conhecidos
                for vend in vendedores_conhecidos:
                    if campo_combinado.upper().endswith(vend):
                        vendedor_extraido = vend
                        cliente_extraido = campo_combinado[:-len(vend)].strip()
                        break

                rows.append({
                    "Data": match.group(1),
                    "Pedido": match.group(2),
                    "Nota Fiscal": match.group(3) if match.group(3) else "",
                    "Cliente": cliente_extraido,
                    "Vendedor": vendedor_extraido,
                    "Vendedor 2": match.group(5),
                    "Condição Pagamento": match.group(6).strip(),
                    "Valor Produto": parse_num(match.group(7)),
                    "Valor Final": parse_num(match.group(8)),
                    "Total IPI": parse_num(match.group(9)),
                    "Markup": parse_num(match.group(10)),
                    "% Atendimento": parse_num(match.group(11))
                })

# Cria o DataFrame
df = pd.DataFrame(rows)
if df.empty:
    raise Exception("Nenhum dado foi extraído do PDF. Verifique se o formato do arquivo é compatível.")

# Converter a coluna Data para datetime (formato dd/mm/yy)
df['Data'] = pd.to_datetime(df['Data'], format='%d/%m/%y')

# Converter os valores numéricos já foram convertidos na função parse_num

# Filtrar: remover as linhas cujo Cliente contenha "comagro" (ignorando maiúsculas/minúsculas)
df_filtrado = df[~df['Cliente'].str.lower().str.contains("comagro")]

# Criar uma coluna para identificar a semana (formato "Ano-WXX", onde WXX é o número da semana)
df_filtrado['Ano_Semana'] = df_filtrado['Data'].dt.strftime('%Y-W%U')

# Agrupar por Vendedor e Semana, somando os valores do "Valor Final"
resumo = df_filtrado.groupby(['Vendedor', 'Ano_Semana'])['Valor Final'].sum().reset_index()

# Salvar em um arquivo Excel com duas abas: "Dados" e "Resumo Semanal"
output_file = file_path.replace(".pdf", ".xlsx")
with pd.ExcelWriter(output_file) as writer:
    df_filtrado.to_excel(writer, sheet_name="Dados", index=False)
    resumo.to_excel(writer, sheet_name="Resumo Semanal", index=False)

print(f"Arquivo Excel gerado com sucesso: {output_file}")