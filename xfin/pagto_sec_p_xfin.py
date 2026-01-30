import os
import sqlite3
import firebirdsql
import email_alert
import pandas as pd
from dotenv import load_dotenv
from datetime import date, datetime

# ==============================================================================
# 1. FUNÇÃO DE CONEXÃO (INALTERADA)
# ==============================================================================
load_dotenv()

DB_CONTROLE = "controle_exportacao.db"

def inicializar_db_controle():
    """Cria a tabela de histórico se não existir."""
    conn = sqlite3.connect(DB_CONTROLE)
    cursor = conn.cursor()
    # Chave única: NumDoc + Fornecedor + Parcela (para evitar duplicidade de boleto)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS historico_pagar (
            id_unico TEXT PRIMARY KEY,
            data_exportacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def ja_foi_exportado(id_unico):
    conn = sqlite3.connect(DB_CONTROLE)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM historico_pagar WHERE id_unico = ?", (id_unico,))
    existe = cursor.fetchone() is not None
    conn.close()
    return existe

def marcar_como_exportado(lista_ids):
    conn = sqlite3.connect(DB_CONTROLE)
    cursor = conn.cursor()
    for id_unico in lista_ids:
        try:
            cursor.execute("INSERT INTO historico_pagar (id_unico) VALUES (?)", (id_unico,))
        except sqlite3.IntegrityError:
            pass # Já existe, segue o baile
    conn.commit()
    conn.close()


def get_firebird_connection():
    try:
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
    except Exception as e:
        print(f"Erro CRÍTICO ao tentar conectar ao banco de dados: {e}")
        print("Verifique as variáveis de ambiente no seu arquivo .env")
        return None

# ==============================================================================
# 2. CARREGAMENTO DO MAPEAMENTO (SECULOS -> XFIN)
# ==============================================================================


def carregar_mapa_contas():
    # Ajuste o nome do arquivo se necessário
    arquivo_cod = "arquivos\\[XFIN] Plano de contas para o xfin.xlsx"
    arquivo_desc = "arquivos\\[XFIN] Descrição contas xfin.xlsx"

    try:
        # Lê como string para preservar códigos como "5.10"
        df_mapa = pd.read_excel(arquivo_cod, dtype=str)
        df_plano = pd.read_excel(arquivo_desc, dtype=str)

        # 1. Cria Dicionário: Código Xfin (1.1) -> Nome Xfin (Venda de Produtos)
        dict_cod_nome = {}
        for _, row in df_plano.iterrows():
            codigo = str(row['Código Xfin']).strip()
            nome = str(row['Nome da Conta']).strip()
            if codigo and codigo.lower() != 'nan':
                dict_cod_nome[codigo] = nome

        # 2. Cria Mapa Final: Nome Seculos -> Nome Xfin
        mapa_final = {}
        for _, row in df_mapa.iterrows():
            nome_seculos = str(row['Seculos']).strip().upper()
            cod_xfin_alvo = str(row['Xfin']).strip()

            # Só adiciona ao mapa se encontrarmos o nome correspondente no Xfin.
            # Se for "ANALISAR" ou código inválido, não entra no mapa,
            # o que fará cair na lista de "PARA ANÁLISE" no main.
            if cod_xfin_alvo in dict_cod_nome:
                nome_final = dict_cod_nome[cod_xfin_alvo]
                mapa_final[nome_seculos] = nome_final

        print(f"Mapeamento carregado: {len(mapa_final)} contas validadas.")
        return mapa_final

    except FileNotFoundError as e:
        print(f"ERRO: Arquivos não encontrados - {e}")
        return {}
    except Exception as e:
        print(f"Erro ao processar mapeamento: {e}")
        return {}

# ==============================================================================
# 3. LÓGICAS DE NEGÓCIO (DATA, DOCUMENTO, BANCO)
# ==============================================================================


def definir_data_emissao(row):
    # Lógica: if CDNOTACOMPRA exists -> NOTACOMPRA.DTEMISSAO
    # elif DTVENCIMENTO < today -> DTVENCIMENTO
    # else today

    dt_emissao_nota = row.get('DTEMISSAO_NOTA')  # Vindo do JOIN com NOTACOMPRA
    dt_vencimento = row.get('DTVENCIMENTO')
    hoje = date.today()

    if row.get('CDNOTACOMPRA') and dt_emissao_nota:
        return dt_emissao_nota
    elif dt_vencimento and dt_vencimento < hoje:
        return dt_vencimento
    else:
        return hoje


def definir_tipo_documento(num_conta_cred, descricao, cd_fornecedor):
    """ Define o Tipo de Documento baseado em regras específicas. Ordem de prioridade de cima pra baixo """

    # Garante que a descrição esteja em minúsculas para a verificação
    desc_lower = str(descricao).lower()

    # 3. Checa Fornecedor (IDs Específicos)
    if cd_fornecedor in [759, 469, 933]:
        return "Vale"
    if cd_fornecedor in [807, 808, 1024]:
        return "Salário"
    if cd_fornecedor in [368, 914, 799]:
        return "Boleto"
    if cd_fornecedor in [1049]:
        return "Cheque"

    # 2. Checa Descrição (Palavras-chave Case Insensitive)
    termos_cartao = ["cartão", "cartao", "hipercard"]
    # Verifica se algum termo está contido na descrição em minúsculas
    if any(termo in desc_lower for termo in termos_cartao):
        return "Cartão de Crédito"

    termos_dev_cred = ["devolução", "devolucao", "estorno", "garantia", "credito", "crédito"]
    if any(termo in desc_lower for termo in termos_dev_cred):
        return "Crédito/Estorno"
    
    termos_emprestimo = ["empréstimo", "emprestimo", "financiamento", "financiamento", "finame"]
    if any(termo in desc_lower for termo in termos_emprestimo):
        return "Débito Automático"

    # 1. Checa NUMCONTACRED
    if num_conta_cred == 8:
        return "NF"
    if num_conta_cred == 6:
        return "PIX"
    if num_conta_cred == 1:
        return "Dinheiro"
    if num_conta_cred == 3:
        return "Boleto"
    if num_conta_cred in [2, 4, 5, 7]:
        return "Débito Automático"

    # 4. Fallback (Padrão)
    return "Dinheiro"


def definir_banco(num_conta_cred):
    # Relação Seculos -> Xfin
    mapa_bancos = {
        1: "Caixa Empresa",
        2: "Bradesco",
        3: "Banco do Brasil Peças",
        4: "Caixa Econômica Federal",
        5: "Sicoob",
        6: "Banco Inter Peças",
        7: "Banco do Nordeste",
        8: "Banco do Brasil Peças",
        9: "Caixa Empresa Serviços",
        10: "Banco Inter Serviços",
        11: "Banco do Brasil Serviços"
    }
    return mapa_bancos.get(num_conta_cred, "")  # Retorna vazio se não achar


def formatar_valor(valor):
    if valor is None:
        return "0,00"
    # Formata float para string com vírgula (padrão Excel BR)
    return f"{valor:.2f}".replace('.', ',')


def formatar_data(data_obj):
    if not data_obj:
        return ""
    return data_obj.strftime("%d/%m/%Y")

# ==============================================================================
# 4. EXTRAÇÃO E EXPORTAÇÃO
# ==============================================================================


def main():
    conn = get_firebird_connection()
    if not conn:
        return None

    mapa_contas = carregar_mapa_contas()
    print("Mapa de contas carregado com sucesso.")

    # 1. Identificar Filiais (Centros de Custo) com movimentos pendentes
    cursor_filiais = conn.cursor()
    sql_filiais = """
    SELECT DISTINCT A.CDCENTRODECUSTO
    FROM APAGAR A
    WHERE A.DTVENCIMENTO >= '2026-01-01'
    """
    cursor_filiais.execute(sql_filiais)
    filiais_encontradas = [row[0] for row in cursor_filiais.fetchall()]
    filiais_encontradas.pop(0)  # Remove a filial 0 (Contas sem filial)
    
    print(f"Filiais encontradas com movimentos: {filiais_encontradas}")
    
    arquivos_gerados_prontos = []
    cursor = conn.cursor()

    # Query SQL otimizada com JOINs
    # Pegamos apenas 10 registros > 2026 conforme solicitado
    sql = """
    SELECT
        A.CDFORNECEDOR,
        A.NOMEFORNECEDOR,
        A.CDNOTACOMPRA,
        N.DTEMISSAO AS DTEMISSAO_NOTA,
        A.DTVENCIMENTO,
        A.VALOR,
        S.NOME AS NOME_SUBSUBCONTA,
        A.NUMCONTACRED,
        A.DESCRICAO,
        A.VALORPAGO,
        A.DTPGTO,
        A.NUMPARCELA,
        A.NUMDOCUMENTO
    FROM APAGAR A
    LEFT JOIN FORNECEDOR F ON A.CDFORNECEDOR = F.CDFORNECEDOR
    LEFT JOIN SUBSUBCONTA S ON A.SUBSUBNUMCONTA = S.SUBSUBNUMCONTA
    LEFT JOIN NOTACOMPRA N ON A.CDNOTACOMPRA = N.CDNOTACOMPRA
    WHERE A.DTVENCIMENTO >= '2026-01-01' AND A.CDCENTRODECUSTO = ?
    ORDER BY A.DTVENCIMENTO ASC
    """

    for cd_filial in filiais_encontradas:
        print(f"\n--- Processando Filial {cd_filial} ---")
        
        cursor.execute(sql, (cd_filial,))
        registros = cursor.fetchall()

        # Listas separadas
        dados_sucesso = []
        dados_analise = []
        ids_para_salvar = [] # Lista temporária de IDs processados com sucesso

        # Obter nomes das colunas para mapear no dict
        colunas = [desc[0] for desc in cursor.description]

        for reg in registros:
            # Cria um dicionário da linha para facilitar acesso
            row = dict(zip(colunas, reg))
            
            # Gera ID Único para evitar duplicidade na exportação
            doc_limpo = str(row['NUMDOCUMENTO']).split('/')[0].strip() if row['NUMDOCUMENTO'] else "S_DOC"
            id_unico = f"DOC_{doc_limpo}_FORN_{row['CDFORNECEDOR']}_PARC_{row['NUMPARCELA']}"
            
            # Verifica no SQLite se já enviamos
            if ja_foi_exportado(id_unico):
                continue

            # --- Processamento Lógico ---

            # 1. Pessoa
            pessoa = row['NOMEFORNECEDOR'] if row['NOMEFORNECEDOR'] else "Consumidor Final"

            # 2. Datas
            dt_emissao = definir_data_emissao(row)
            dt_vencimento = row['DTVENCIMENTO']

            # --- Definição do Plano de Contas ---
            nome_conta_seculos = str(row['NOME_SUBSUBCONTA']).strip().upper()

            encontrado = False

            if nome_conta_seculos in mapa_contas:
                plano_contas_final = mapa_contas[nome_conta_seculos]  # Usa o NOME DO XFIN
                encontrado = True
            else:
                plano_contas_final = nome_conta_seculos  # Mantém ORIGINAL para análise
                encontrado = False

            # 4. Tipo de Documento
            tipo_doc = definir_tipo_documento(
                row['NUMCONTACRED'],
                row['DESCRICAO'],
                row['CDFORNECEDOR']
            )

            # 5. Banco
            banco = definir_banco(row['NUMCONTACRED'])

            # 6. Valores
            valor_nominal = row['VALOR']
            valor_pago = row['VALORPAGO'] if row['VALORPAGO'] and row['VALORPAGO'] > 0 else None
            dt_pagamento = row['DTPGTO'] if row['DTPGTO'] else None

            # Limpa as parcelas do número do documento
            doc_original = row['NUMDOCUMENTO']
            if doc_original:
                num_doc = str(doc_original).split('/')[0].strip()
            else:
                num_doc = ""

            # Montando a linha para o CSV
            item = {
                "Pessoa*": pessoa,
                "Emissao*": formatar_data(dt_emissao),
                "Vencimento*": formatar_data(dt_vencimento),
                "Valor*": formatar_valor(valor_nominal),
                "Plano Contas*": plano_contas_final,
                "Tipo Documento*": tipo_doc,
                "Valor Pago": formatar_valor(valor_pago) if valor_pago else "",
                "Data Pagamento": formatar_data(dt_pagamento),
                "Conta/Banco": banco,
                "Parcela": row['NUMPARCELA'],
                "Número Documento": num_doc,
                "Descrição": row['DESCRICAO']
            }

            # Inserção direta na lista correta (sem variável 'destino')
            if encontrado:
                dados_sucesso.append(item)
                ids_para_salvar.append(id_unico)
            else:
                dados_analise.append(item)

        # --- Geração do CSV ---

        # Garante a ordem das colunas conforme seu modelo
        colunas_finais = [
            "Pessoa*", "Emissao*", "Vencimento*", "Valor*", "Plano Contas*",
            "Tipo Documento*", "Valor Pago", "Data Pagamento", "Conta/Banco",
            "Parcela", "Número Documento", "Descrição"
        ]

        # 1. Arquivo PRONTO (Sucesso) - Separador Ponto e Vírgula
        if dados_sucesso:
            df_sucesso = pd.DataFrame(dados_sucesso)
            df_sucesso = df_sucesso.reindex(columns=colunas_finais)
            nome_arq_sucesso = f"arquivos/importacao_xfin_filial_{cd_filial}_PRONTO.csv"
            df_sucesso.to_csv(nome_arq_sucesso, index=False, sep=';', encoding='utf-8-sig')
            
            marcar_como_exportado(ids_para_salvar) # Persiste no SQLite
            arquivos_gerados_prontos.append(nome_arq_sucesso)
            print(f"SUCESSO: '{nome_arq_sucesso}' gerado com {len(dados_sucesso)} registros.")

        # 2. Arquivo PARA ANÁLISE (Erros/Faltantes) - Separador Ponto e Vírgula
        if dados_analise:
            df_analise = pd.DataFrame(dados_analise)
            df_analise = df_analise.reindex(columns=colunas_finais)
            nome_arq_analise = f"arquivos/importacao_xfin_filial_{cd_filial}_PARA_ANALISE.csv"
            df_analise.to_csv(nome_arq_analise, index=False, sep=';', encoding='utf-8-sig')
            
            # Envia e-mail de alerta IMEDIATAMENTE para esta filial
            email_alert.enviar_email_erro(nome_arq_analise, len(dados_analise))
            print(f"ATENÇÃO: '{nome_arq_analise}' gerado com {len(dados_analise)} registros para revisão.")

    conn.close()
    return arquivos_gerados_prontos


if __name__ == "__main__":
    main()
