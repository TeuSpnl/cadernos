import unicodedata
from tkinter import Tk
from tkinter.filedialog import askopenfilename

def remove_accents(text):
    """
    Remove os acentos (diacríticos) de uma string usando a normalização Unicode.
    """
    nfkd_form = unicodedata.normalize('NFKD', text)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])

def process_line(line):
    """
    Processa uma linha do arquivo:
    1. Remove os acentos.
    2. Se a linha tiver ao menos 88 caracteres, verifica o campo de variação
       (posições 86 a 88, índice 85 a 88 em Python). Se for '000', substitui por '019'.
    """
    # Remove acentos da linha inteira
    line = remove_accents(line)
    
    # Verifica se a linha possui comprimento suficiente para ter o campo de variação
    if len(line) >= 88:
        # Posições 86 a 88 (1-indexed) correspondem a índices 85 a 88 (exclusivo)
        if line[85:88] == "000":
            line = line[:85] + "019" + line[88:]
    return line

def process_file(input_filename, output_filename):
    """
    Lê o arquivo de entrada, processa cada linha e grava as alterações
    em um novo arquivo.
    """
    with open(input_filename, 'r', encoding='ISO8859_1') as fin, \
         open(output_filename, 'w', encoding='ISO8859_1') as fout:
        for line in fin:
            # Remove a quebra de linha e processa a linha
            processed_line = process_line(line.rstrip('\n'))
            fout.write(processed_line + '\n')

def choose_file():
    Tk().withdraw()  # Evita que a janela principal do Tkinter apareça
    filename = askopenfilename(title="Selecione o arquivo de entrada")
    return filename

input_file = choose_file()
if not input_file:
    print("Nenhum arquivo selecionado. Saindo...")
    exit()
    
print("input: ", input_file)
out = input_file.split('.')
output_file = out[0] + "-solved.rem"
print("output: ", output_file)

process_file(input_file, output_file)
print(f"Arquivo processado. Saída em: {output_file}")