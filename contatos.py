import pandas as pd
import re
import os

files = ['contacts-1.csv', 'contacts-2.csv', 'contacts-3.csv']
dfs = []

print("Gerando arquivo Excel (.xlsx)...")

for f in files:
    if os.path.exists(f):
        try:
            df = pd.read_csv(f, keep_default_na=False, dtype=str, on_bad_lines='skip')
            df.columns = [c.strip() for c in df.columns]
            dfs.append(df)
        except Exception as e:
            print(f"Erro ao ler {f}: {e}")

if dfs:
    full_df = pd.concat(dfs, ignore_index=True)

    # Limpeza de Nome
    def clean_name(x):
        if pd.isna(x) or x == "":
            return ""
        x = str(x)
        s = re.sub(r'[^\w\s\-]', '', x)
        s = s.replace('_', ' ')
        return re.sub(r'\s+', ' ', s).strip()

    # Formatação de Telefone
    def format_phone(x):
        if pd.isna(x) or x == "":
            return None
        x = str(x)
        digits = re.sub(r'\D', '', x)
        
        if not digits:
            return None
            
        # Brasil
        if len(digits) in [10, 11]:
            digits = '55' + digits
        
        if digits.startswith('55') and len(digits) in [12, 13]:
            if len(digits) == 13: # Celular
                return f"+{digits[:2]} {digits[2:4]} {digits[4:9]}-{digits[9:]}"
            else: # Fixo
                return f"+{digits[:2]} {digits[2:4]} {digits[4:8]}-{digits[8:]}"
        
        # Estrangeiros
        return f"+{digits}"

    # Aplica
    full_df['Name'] = full_df['First Name'].apply(clean_name) if 'First Name' in full_df.columns else ""
    full_df['Mobile Number * (with country code)'] = full_df['Phone 1 - Value'].apply(format_phone) if 'Phone 1 - Value' in full_df.columns else None

    # Filtra e remove duplicatas
    result_df = full_df.dropna(subset=['Mobile Number * (with country code)'])
    result_df = result_df.drop_duplicates(subset=['Mobile Number * (with country code)'])
    
    # Prepara saída
    final_output = result_df[['Mobile Number * (with country code)', 'Name']]
    final_output = final_output.sort_values(by='Name')
    
    # Salva em XLSX
    output_filename = 'lista_clientes_final.xlsx'
    final_output.to_excel(output_filename, index=False)
    
    print(f"Sucesso! Arquivo '{output_filename}' pronto com {len(final_output)} contatos.")
else:
    print("Algo deu errado ao ler os arquivos.")