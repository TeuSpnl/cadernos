import os
import glob
import pandas as pd
import sqlite3
from dotenv import load_dotenv
from pagto_sec_p_xfin import get_firebird_connection, inicializar_db_controle, DB_CONTROLE

load_dotenv()

def carregar_titulos_firebird():
    """
    Carrega todos os títulos do Firebird para criar um mapa de identificação.
    Chave: (NumeroDocLimpo, Parcela, ValorFormatado)
    Valor: ID_UNICO (usado no controle_exportacao.db)
    """
    print("Conectando ao Firebird para buscar títulos...")
    conn = get_firebird_connection()
    if not conn:
        print("Erro de conexão com Firebird.")
        return {}
    
    cursor = conn.cursor()
    # Busca títulos de um período abrangente para garantir que encontramos os dados do XFIN
    # Ajuste a data conforme a antiguidade dos dados no XFIN, se necessário
    sql = """
    SELECT 
        A.CDFORNECEDOR, 
        A.NUMDOCUMENTO, 
        A.NUMPARCELA, 
        A.VALOR
    FROM APAGAR A
    WHERE A.DTVENCIMENTO >= '2024-01-01'
    """
    
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
    except Exception as e:
        print(f"Erro ao consultar Firebird: {e}")
        conn.close()
        return {}
    
    conn.close()
    
    lookup = {}
    for row in rows:
        cd_forn, num_doc, num_parc, valor = row
        
        # Lógica de limpeza idêntica ao script principal
        doc_limpo = str(num_doc).split('/')[0].strip() if num_doc else "S_DOC"
        
        # ID Único que queremos salvar no SQLite
        id_unico = f"DOC_{doc_limpo}_FORN_{cd_forn}_PARC_{num_parc}"
        
        # Chave para encontrar este título usando dados do CSV do XFIN
        # Usamos Valor com 2 casas decimais para evitar erros de arredondamento
        val_str = f"{float(valor):.2f}"
        parc_str = str(num_parc).strip()
        
        # Chave composta: (Documento, Parcela, Valor)        
        key = (doc_limpo, parc_str, val_str)
        lookup[key] = id_unico
        
    print(f"Mapeados {len(lookup)} títulos do Firebird para conferência.")
    return lookup

def normalizar_valor(valor_str):
    """Converte string de valor (ex: '1.200,50' ou '1200.50') para string '1200.50'"""
    if pd.isna(valor_str): return "0.00"
    v = str(valor_str).strip()
    v = v.replace('R$', '').strip()
    
    # Tenta detectar formato brasileiro (vírgula como decimal)
    if ',' in v and '.' in v:
        v = v.replace('.', '').replace(',', '.')
    elif ',' in v:
        v = v.replace(',', '.')
    
    try:
        return f"{float(v):.2f}"
    except:
        return "0.00"

def processar_arquivos_xfin():
    inicializar_db_controle()
    mapa_fb = carregar_titulos_firebird()
    if not mapa_fb: return

    lista_arquivos = glob.glob("arquivos/*.csv")
    if not lista_arquivos:
        print("Nenhum arquivo .csv encontrado na pasta 'arquivos'.")
        return

    conn_sqlite = sqlite3.connect(DB_CONTROLE)
    cursor_sqlite = conn_sqlite.cursor()
    total_inseridos = 0
    
    for arquivo in lista_arquivos:
        print(f"\nProcessando arquivo: {arquivo}")
        try:
            df = pd.read_csv(arquivo, sep=None, engine='python', dtype=str, encoding='latin1')
            df.columns = [c.lower().strip() for c in df.columns]
            
            col_doc = next((c for c in df.columns if 'número documento' in c), None)
            col_parc = next((c for c in df.columns if 'parcela' in c), None)
            col_valor = next((c for c in df.columns if 'valor' in c and 'pago' not in c), None)
            
            if not (col_doc and col_parc and col_valor):
                print(f"  [!] Colunas não identificadas em {arquivo}. Pulando.")
                continue
            
            for _, row in df.iterrows():
                doc_limpo = str(row[col_doc]).split('/')[0].strip()
                parc_limpa = str(row[col_parc]).split('/')[0].strip()
                val_csv = normalizar_valor(row[col_valor])
                
                print(f"  Verificando: Doc='{doc_limpo}', Parc='{parc_limpa}', Valor='{val_csv}'")
                
                if (doc_limpo, parc_limpa, val_csv) in mapa_fb:
                    id_unico = mapa_fb[(doc_limpo, parc_limpa, val_csv)]
                    try:
                        cursor_sqlite.execute("INSERT INTO historico_pagar (id_unico) VALUES (?)", (id_unico,))
                        total_inseridos += 1
                    except sqlite3.IntegrityError: pass
        except Exception as e: print(f"  Erro: {e}")

    conn_sqlite.commit()
    conn_sqlite.close()
    print(f"\nConcluído. {total_inseridos} registros marcados como já exportados.")

if __name__ == "__main__":
    processar_arquivos_xfin()