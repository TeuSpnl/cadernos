import os
import firebirdsql
import pandas as pd
from dotenv import load_dotenv
from datetime import date, datetime, timedelta

# ==============================================================================
# CONFIGURAÇÕES GERAIS
# ==============================================================================
load_dotenv()
# DIAS_RETROATIVOS = 45
# Busca no banco tudo desde o ano passado para pegar parcelas longas
DATA_BUSCA_SQL = '2025-01-01' 

# O que realmente vai para o arquivo (Vencimentos ou Pagamentos a partir desta data)
DATA_CORTE_XFIN = date(2026, 1, 1)

MAPA_PAGAMENTO = {
    # --- DINHEIRO E CAIXA ---
    1:  {'Tipo': 'Dinheiro', 'Natureza': 'Vista', 'Conta': 'Caixa Empresa'},
    33: {'Tipo': 'Dinheiro', 'Natureza': 'Vista', 'Conta': 'Caixa Empresa Serviços'},  # Dinheiro Serviços

    # --- CRÉDITO (A vista) ---
    19: {'Tipo': 'Crédito/Estorno', 'Natureza': 'Vista', 'Conta': 'Caixa Empresa'},  # Crédito

    # --- CARTÕES (A lógica 'processar_cartao' tem prioridade, isso é fallback) ---
    4:  {'Tipo': 'Cartão de Crédito', 'Natureza': 'Cartao', 'Conta': 'Banco do Brasil Peças'},  # Cartão
    28: {'Tipo': 'Cartão de Crédito', 'Natureza': 'Cartao', 'Conta': 'Banco do Brasil Peças'},  # Cartão+Cartão
    35: {'Tipo': 'Cartão de Crédito', 'Natureza': 'Cartao', 'Conta': 'Banco do Brasil Serviços'},  # Cartão Serviços

    # --- A PRAZO (FIADO / CARTEIRA) ---
    5:  {'Tipo': 'Fatura', 'Natureza': 'Prazo', 'Conta': 'Banco do Brasil Peças'},  # O famoso "Pedido"
    26: {'Tipo': 'Fatura', 'Natureza': 'Prazo', 'Conta': 'Banco do Brasil Peças'},  # Pedido 2
    30: {'Tipo': 'Fatura', 'Natureza': 'Prazo', 'Conta': ''},  # Oficina
    31: {'Tipo': 'Fatura', 'Natureza': 'Prazo', 'Conta': ''},  # Parceiro

    # --- MISTOS COM PEDIDO (FRANKENSTEINS 🧟‍♂️ -> TRATAR COMO PRAZO) ---
    10: {'Tipo': 'Fatura', 'Natureza': 'Prazo', 'Conta': 'Banco do Brasil Peças'},  # Din + Pedido
    14: {'Tipo': 'Fatura', 'Natureza': 'Prazo', 'Conta': 'Banco do Brasil Peças'},  # Cheque + Pedido
    15: {'Tipo': 'Fatura', 'Natureza': 'Prazo', 'Conta': 'Banco do Brasil Peças'},  # Cartão + Pedido
    22: {'Tipo': 'Fatura', 'Natureza': 'Prazo', 'Conta': 'Banco do Brasil Peças'},  # Credito + Pedido

    # --- CHEQUES (Geralmente é a prazo/custódia) ---
    6:  {'Tipo': 'Cheque', 'Natureza': 'Prazo', 'Conta': 'Banco do Brasil Peças'},  # Cheque

    # --- DEPÓSITOS / PIX / TRANSFERÊNCIAS (BANCOS ESPECÍFICOS) ---
    8:  {'Tipo': 'Depósito Bancário', 'Natureza': 'Vista', 'Conta': 'Banco do Brasil Peças'},  # Dep Brasil
    11: {'Tipo': 'Fatura', 'Natureza': 'Prazo', 'Conta': 'Banco do Brasil Peças'},  # Receb Brasil
    36: {'Tipo': 'Depósito Bancário', 'Natureza': 'Vista', 'Conta': 'Banco do Brasil Serviços'},  # Dep Brasil Serviços
    37: {'Tipo': 'Fatura', 'Natureza': 'Prazo', 'Conta': 'Banco do Brasil Serviços'},  # Receb Brasil Serviço

    9:  {'Tipo': 'Depósito Bancário', 'Natureza': 'Vista', 'Conta': 'Bradesco'},  # Dep Bradesco
    13: {'Tipo': 'Fatura', 'Natureza': 'Prazo', 'Conta': 'Bradesco'},  # Receb Bradesco

    27: {'Tipo': 'PIX', 'Natureza': 'Vista', 'Conta': 'Banco Inter Peças'},  # Depósito Bancário Inter
    32: {'Tipo': 'Fatura', 'Natureza': 'Prazo', 'Conta': 'Banco Inter Peças'},  # Receb Inter
    34: {'Tipo': 'PIX', 'Natureza': 'Vista', 'Conta': 'Banco Inter Serviços'},  # Depósito Bancário Serviços

    23: {'Tipo': 'Depósito Bancário', 'Natureza': 'Vista', 'Conta': 'Caixa Econômica Federal'},  # Dep CEF
    24: {'Tipo': 'Depósito Bancário', 'Natureza': 'Vista', 'Conta': 'Sicoob'},  # Dep Sicoob

    # --- OUTROS ---
    18: {'Tipo': 'Outros', 'Natureza': 'Prazo', 'Conta': ''},  # Acerto Renegociado
}


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
# FUNÇÕES DE FORMATAÇÃO E AUXILIARES
# ==============================================================================


def formatar_data_br(data_obj):
    if not data_obj:
        return ""
    return data_obj.strftime("%d/%m/%Y")


def formatar_valor_br(val):
    # Se for None ou string vazia, retornamos vazio
    if val is None or val == "":
        return ""

    # Se for string, tentamos converter para float antes de formatar.
    if isinstance(val, str):
        try:
            # Troca vírgula por ponto para garantir conversão
            val = float(val.replace(',', '.'))
        except ValueError:
            # Se não for número (ex: texto sujo), retorna como está para não quebrar o fluxo
            return val

    # Depois de garantir que é número, aplicamos a formatação brasileira
    return f"{val:.2f}".replace('.', ',')


def definir_plano_contas(valor_produtos, valor_total):
    # Se tem valor de produto > 0, consideramos Venda de Produtos (Loja)
    if valor_produtos and valor_produtos > 0:
        return "Venda de produtos"
    return "Prestação de serviços"

# ==============================================================================
# LÓGICA 1: FATURADOS (DUPLICATADAFATURA)
# ==============================================================================


def processar_faturado(conn, cd_fatura, num_pedido):
    cursor = conn.cursor()

    sql = """
    SELECT 
        D.NUMDUPLICATA, D.VALOR, D.DTVENCIMENTO, D.PAGO, 
        D.DTULTPGTO, D.JUROSTOTALPAGO, D.DTSITUACAO
    FROM DUPLICATADAFATURA D
    WHERE D.CDFATURA = ?
    ORDER BY D.DTVENCIMENTO
    """
    cursor.execute(sql, (cd_fatura,))
    duplicatas = cursor.fetchall()

    titulos = []
    if not duplicatas:
        return [], False

    total_parcelas = len(duplicatas)

    for i, dup in enumerate(duplicatas):
        num_dup, valor, dt_venc, pago, dt_pgto, juros_pagos, dt_emissao = dup
        num_dup = str(num_dup).split('/')[0].strip()
        valor = float(valor)
        juros_pagos = float(juros_pagos) if juros_pagos else 0.0
        valor_pago_final = ""
        data_pgto_final = ""

        if pago == 'S' or (dt_pgto is not None):
            valor_recebido = valor + juros_pagos
            valor_pago_final = valor_recebido
            data_pgto_final = dt_pgto if dt_pgto else dt_venc

        if valor_pago_final:
            descricao = f"Receb. NF {num_dup}, Parc {i+1}/{total_parcelas}"
        else:
            descricao = f"Fatura NF {num_dup}, Parc {i+1}/{total_parcelas}"

        titulos.append({
            "EmissaoReal": dt_emissao,
            "Vencimento": dt_venc,
            "Valor": valor,
            "TipoDoc": "Fatura",
            "ValorPago": valor_pago_final,
            "DataPagamento": data_pgto_final,
            "Conta": "Banco Brasil Peças" if valor_pago_final else "",
            "Parcela": f"{i+1}",
            "NumeroDoc": f"{num_dup}",
            "Descricao": descricao
        })

    return titulos, True

# ==============================================================================
# LÓGICA 2: CARTÕES
# ==============================================================================


def processar_cartao(conn, cd_pedido, dt_venda_original, num_doc_ou_pedido, nome_cliente, nome_conta):
    cursor = conn.cursor()

    sql_head = """
    SELECT 
        P.CDPAGAMENTOACARTAO, P.CREDITODEBITO 
    FROM PAGAMENTOSACARTAO P 
    WHERE P.CDPEDIDOVENDA = ?
        OR P.CDRECEBIMENTO = ? 
    """
    cursor.execute(sql_head, (cd_pedido, num_doc_ou_pedido))
    pagto_cartao = cursor.fetchone()

    if not pagto_cartao:
        print(f"Nenhum pagamento por cartão encontrado para o pedido {cd_pedido}.")
        return [], False

    cd_pagto_cartao, tipo_cd = pagto_cartao

    # Se for DÉBITO -> Retorna flag True para tratar no Main
    if tipo_cd == 'D':
        return [], True

    # Se for CRÉDITO -> Busca Parcelas
    sql_parcelas = """
    SELECT 
        PC.PARCELANUM, PC.VALOR, PC.DTVENCIMENTO, 
        PC.LANCADO, PC.DTCOMPENSADO 
    FROM PARCELASCARTAO PC 
    WHERE PC.CDPAGAMENTOACARTAO = ? 
    ORDER BY PC.PARCELANUM
    """
    cursor.execute(sql_parcelas, (cd_pagto_cartao,))
    parcelas = cursor.fetchall()

    if not parcelas:
        return [], True  # Fallback

    titulos = []

    qtd_parcelas = len(parcelas)
    for p in parcelas:
        num_parc, valor, dt_ven, lancado, dt_comp = p

        status_pgto = ""
        dt_baixa = ""

        if lancado == 'S' and dt_comp:
            status_pgto = valor
            dt_baixa = dt_comp

        titulos.append({
            "Vencimento": dt_ven,
            "Valor": valor,
            "TipoDoc": "Cartão de Crédito",
            "ValorPago": status_pgto,
            "DataPagamento": dt_baixa,
            "Conta": nome_conta if status_pgto else "",
            "Parcela": f"{num_parc}",
            "NumeroDoc": f"{num_doc_ou_pedido}",
            "Descricao": f"Cartão Ped {num_doc_ou_pedido} Parc {num_parc}/{qtd_parcelas}"
        })

    return titulos, False

# ==============================================================================
# LÓGICA 3: FIADO / A PRAZO / AVULSOS
# ==============================================================================


def verificar_recebimento_carteira(conn, cd_pedido, cd_nota, nome_cliente=""):
    """
    Verifica se um pedido a prazo foi pago na tabela RECEBIMENTOS.
    """
    cursor = conn.cursor()

    # Se temos a Nota, o caminho é mais preciso
    if cd_nota:
        sql = """
        SELECT 
            R.DATA, 
            I.VALORREAL, -- Pegamos o valor amortizado do item, não o total do recibo
            R.CDFORMAPAG,
            R.VALORTOTAL,
            R.CDRECEBIMENTO
        FROM ITENSRECEBIMENTO I
        JOIN RECEBIMENTO R ON I.CDRECEBIMENTO = R.CDRECEBIMENTO
        WHERE I.CDNOTA = ?
        """
        cursor.execute(sql, (cd_nota,))

    else:
        # Fallback: tenta pelo pedido se não tiver nota
        sql = """
        SELECT 
            R.DATA, 
            I.VALORREAL,
            R.CDFORMAPAG,
            R.VALORTOTAL,
            R.CDRECEBIMENTO
        FROM ITENSRECEBIMENTO I
        JOIN RECEBIMENTO R ON I.CDRECEBIMENTO = R.CDRECEBIMENTO
        WHERE I.CDPEDIDOVENDA = ?
        """
        cursor.execute(sql, (cd_pedido,))

    recebimentos = cursor.fetchall()
    pagamentos_detectados = []

    if recebimentos:
        for rec in recebimentos:
            data_rec, valor_item, cd_forma_real, total_recibo, cd_recebimento = rec

            # Ajuste de segurança para valor
            valor_final = float(valor_item) if valor_item else 0.0
            if valor_final <= 0:
                continue

            titulos = resolver_pagamento(conn, cd_forma_real, valor_final, data_rec, cd_pedido,
                                         cd_recebimento, nome_cliente, f"Receb. Baixa Pedido {cd_pedido}")

            pagamentos_detectados.extend(titulos)

    return pagamentos_detectados


def cliente_costuma_pagar_boleto(conn, cd_cliente):
    """
    Verifica se o cliente tem histórico recente de pagamento via Boleto (11).
    Isso indica que ele é 'Faturado' e não devemos lançar pedidos avulsos.
    """
    # Define limite de 60 dias atrás para considerar o cliente "ativo no boleto"
    data_limite = date.today() - timedelta(days=60)

    cursor = conn.cursor()
    # Usando RECEBIMENTOS (Plural, conforme padrão do banco)
    sql = """
    SELECT FIRST 1 1 
    FROM RECEBIMENTO
    WHERE CDCLIENTE = ? 
      AND CDFORMAPAG = 11 
      AND DATA >= ?
    """
    cursor.execute(sql, (cd_cliente, data_limite))
    return cursor.fetchone() is not None

# ==============================================================================
# NOVA FUNÇÃO UNIFICADA: O CORAÇÃO DO PAGAMENTO
# ==============================================================================


def resolver_pagamento(conn, cd_forma, valor, dt_venda, cd_pedido, num_doc_ou_pedido, nome_cli, contexto_desc=""):
    """
    Função Universal: Recebe um valor e uma forma de pagamento e decide como transformar
    isso em títulos (seja cartão, pix, dinheiro, etc).

    Usa o MAPA_PAGAMENTO como guia.
    """

    if valor <= 0.01:
        return []

    # 1. Busca Configuração no Mapa
    config = MAPA_PAGAMENTO.get(cd_forma)
    if not config:
        # Tenta fallback para Misto ou Outros
        if cd_forma == 16:
            config = {'Tipo': 'Misto', 'Natureza': 'Misto', 'Conta': ''}
        else:
            config = {'Tipo': 'Outros', 'Natureza': 'Vista', 'Conta': ''}

    natureza = config['Natureza']
    conta_destino = config['Conta']
    tipo_doc_original = config['Tipo']

    titulos_gerados = []

    if cd_forma == 16:
        # Passo A: Tenta extrair a parte do Cartão
        titulos_card, eh_debito_ou_falha = processar_cartao(
            conn, cd_pedido, dt_venda, num_doc_ou_pedido, nome_cli, "Banco do Brasil Peças")

        soma_cartao = 0.0
        if titulos_card:
            titulos_gerados.extend(titulos_card)
            # Soma o que já foi processado como cartão para achar a diferença
            for item in titulos_card:
                # Converte de volta de string para float para somar (cuidado com formatação)
                if isinstance(item['Valor'], (int, float)):
                    soma_cartao += item['Valor']

        # Passo B: O que sobrou é Dinheiro
        diferenca_dinheiro = valor - soma_cartao

        if diferenca_dinheiro > 0:
            titulos_gerados.append({
                "Vencimento": dt_venda,
                "Valor": diferenca_dinheiro,
                "TipoDoc": "Dinheiro",
                "ValorPago": diferenca_dinheiro,
                "DataPagamento": dt_venda,
                "ContaPagamento": "Caixa Empresa",
                "Descricao": f"Venda Dinheiro (Misto) Pedido {num_doc_ou_pedido}"
            })

            return titulos_gerados

    # 2. Tenta Processar como Cartão (Se a natureza mandar)
    eh_debito_ou_falha = False

    if natureza == 'Cartao':
        # Tenta buscar os detalhes na tabela de cartão
        # OBS: Se for recebimento de carteira, passamos a data do recebimento como base
        titulos_card, eh_debito_ou_falha = processar_cartao(
            conn, cd_pedido, dt_venda, num_doc_ou_pedido, nome_cli, conta_destino
        )

        if titulos_card:
            # Se achou parcelas de crédito, retorna elas
            return titulos_card

        # Se não achou (eh_debito_ou_falha=True), cai para a lógica de baixo
        # mas mantendo a conta definida para cartão

    # 3. Processamento Genérico (Vista, Débito, Pix, Dinheiro)
    # Se for Vista OU se for um Cartão que falhou na busca de parcelas (Débito)
    if natureza == 'Vista' or eh_debito_ou_falha:
        dt_venc = dt_venda
        dt_pgto = dt_venda
        tipo_final = tipo_doc_original

        if eh_debito_ou_falha:
            dt_venc = dt_venda + timedelta(days=1)
            dt_pgto = dt_venc
            tipo_final = "Cartão de Débito"

        titulos_gerados.append(
            {"Vencimento": dt_venc,
             "Valor": valor,
             "TipoDoc": tipo_final,
             "ValorPago": valor,
             "DataPagamento": dt_pgto,
             "Conta": conta_destino,
            "Descricao": f"{contexto_desc} - {tipo_final} "
             if contexto_desc else f"Venda {tipo_final}  Pedido {num_doc_ou_pedido} ", })

    return titulos_gerados

# ==============================================================================
# MAIN (MOTOR PRINCIPAL)
# ==============================================================================


def main():
    conn = get_firebird_connection()
    if not conn:
        return

    # Filtro de Data Fixa
    print(f"--- Iniciando Migração de Contas a Receber (Apenas LOJA) ---")
    print(f"Data de Corte: {DATA_BUSCA_SQL}")

    cursor = conn.cursor()

    sql_pedidos = f"""
    SELECT 
        P.CDPEDIDOVENDA, P.CDPEDIDOVENDA, P.DATA, P.VALORCDESC, 
        P.CDFORMAPAG, P.CDCLIENTE, C.NOME,
        N.CDFATURA, N.CDNOTA, P.VALORTOTALPRODUTOS
    FROM PEDIDOVENDA P
    LEFT JOIN CLIENTE C ON P.CDCLIENTE = C.CDCLIENTE
    LEFT JOIN NOTA N ON P.CDPEDIDOVENDA = N.CDPEDIDOVENDA
    WHERE P.EFETIVADO = 'S' 
      AND (P.DEVOLVIDO IS NULL OR P.DEVOLVIDO <> 'S')
      AND P.DATA >= '{DATA_BUSCA_SQL}'
      AND P.CDFUNC NOT IN (2621, 3224)
      AND P.VALORTOTALPRODUTOS > 0
      AND P.NOMECLIENTE NOT LIKE '%COMAGRO%'
    ORDER BY P.DATA ASC
    """

    cursor.execute(sql_pedidos)
    pedidos = cursor.fetchall()

    print(f"Pedidos encontrados: {len(pedidos)}")

    dados_exportacao = []

    # --- CACHE PARA NÃO CONSULTAR O MESMO CLIENTE MIL VEZES ---
    cache_clientes_faturados = {}

    for ped in pedidos:
        cd_pedido, num_pedido, dt_venda, total, cd_forma, cd_cli, nome_cli, cd_fatura, cd_nota, val_prod = ped
        total = float(total)

        if not nome_cli:
            nome_cli = "ERRO - SEM NOME"
        plano_conta_desc = definir_plano_contas(val_prod, total)

        titulos_do_pedido = []

        # Pedidos faturados
        if cd_fatura and cd_fatura > 0:
            titulos_fat, faturado_ok = processar_faturado(conn, cd_fatura, num_pedido)
            if faturado_ok:
                titulos_do_pedido.extend(titulos_fat)
                # Se achou faturado, não precisa verificar outras lógicas de forma de pgto
                # A menos que seja mista, mas sua lógica original dava 'continue', mantivemos assim.
                pass
            else:
                # Se tinha código de fatura mas não achou parcelas (estranho), cai pro resto
                pass

        if not titulos_do_pedido:
            titulos_recuperados = verificar_recebimento_carteira(conn, cd_pedido, cd_nota, nome_cli)

            if titulos_recuperados:
                titulos_do_pedido.extend(titulos_recuperados)

        if not titulos_do_pedido:
            # Carregar Configuração
            config = MAPA_PAGAMENTO.get(cd_forma)
            if not config:
                config = {'Natureza': 'Prazo' if cd_forma != 16 else 'Misto'}
            natureza = config['Natureza']

            # CASO VISTA OU CARTÃO
            if natureza in ['Vista', 'Cartao']:
                titulos_do_pedido = resolver_pagamento(
                    conn, cd_forma, total, dt_venda, cd_pedido, num_pedido, nome_cli, f"Venda Pedido {num_pedido}"
                )

            # CASO PRAZO
            elif natureza == 'Prazo':
                titulos_pagos = verificar_recebimento_carteira(conn, cd_pedido, cd_nota, nome_cli)

                if titulos_pagos:
                    titulos_do_pedido.extend(titulos_pagos)
                else:
                    # Verifica Cache
                    if cd_cli not in cache_clientes_faturados:
                        cache_clientes_faturados[cd_cli] = cliente_costuma_pagar_boleto(conn, cd_cli)

                    # Se não paga boleto, gera previsão de carteira
                    if not cache_clientes_faturados[cd_cli]:
                        titulos_do_pedido.append({
                            "Vencimento": dt_venda + timedelta(days=7),
                            "Valor": total,
                            "TipoDoc": "Fatura",
                            "ValorPago": "", "DataPagamento": "", "Conta": "",
                            "Descricao": f"Venda Carteira Pedido {num_pedido}",
                            "Parcela": "1",
                            "NumeroDoc": str(num_pedido)
                        })
                    # Se paga boleto e não achou fatura lá em cima, provavelmente é um erro de cadastro ou delay

        for item in titulos_do_pedido:
            dt_venc = item['Vencimento']
            dt_pgto = item['DataPagamento']
            
            # FILTRO DE DATA:
            # Só entra se o pagamento foi depois da data de corte ou, caso não haja pagamento, se o vencimento for depois da data de corte
            impacta_ano_atual = False
            
            if not dt_pgto:
                if dt_venc >= DATA_CORTE_XFIN:
                    impacta_ano_atual = True
            elif dt_pgto >= DATA_CORTE_XFIN:
                impacta_ano_atual = True
                
            if impacta_ano_atual:
                # Se vier do processar_faturado, usa a EmissaoReal (DTSITUACAO)
                # Se não, usa a data da venda do pedido mesmo
                emissao_final = item.get('EmissaoReal', dt_venda)
            
                dados_exportacao.append({
                    "Pessoa": nome_cli,
                    "Emissao": emissao_final,
                    "Vencimento": item['Vencimento'],
                    "Valor": item['Valor'],
                    "Plano Contas": plano_conta_desc,
                    "Tipo Documento": item['TipoDoc'],
                    "Valor Pago": item['ValorPago'],
                    "Data Pagamento": item['DataPagamento'],
                    "Conta/Banco": item['Conta'],
                    "Parcela": item.get('Parcela', "1"),
                    "Número Documento": item.get('NumeroDoc', str(num_pedido)),
                    "Descrição": item['Descricao']
                })

    conn.close()

    if dados_exportacao:
        df = pd.DataFrame(dados_exportacao)

        colunas_data = ['Emissao', 'Vencimento', 'Data Pagamento']
        for col in colunas_data:
            df[col] = df[col].apply(formatar_data_br)

        colunas_valor = ['Valor', 'Valor Pago']
        for col in colunas_valor:
            df[col] = df[col].apply(formatar_valor_br)

        mapa_colunas = {
            "Pessoa": "Pessoa*",
            "Emissao": "Emissao*",
            "Vencimento": "Vencimento*",
            "Valor": "Valor*",
            "Plano Contas": "Plano Contas*",
            "Tipo Documento": "Tipo Documento*",
            "Valor Pago": "Valor Pago",
            "Data Pagamento": "Data Pagamento",
            "Conta/Banco": "Conta/Banco",
            "Parcela": "Parcela",
            "Número Documento": "Número Documento",
            "Descrição": "Descrição"
        }
        df = df.rename(columns=mapa_colunas)

        for col in mapa_colunas.values():
            if col not in df.columns:
                df[col] = ""

        df = df[list(mapa_colunas.values())]

        nome_arq = "arquivos/importacao_RECEITAS_xfin_LOJA.csv"
        df.to_csv(nome_arq, index=False, sep=';', encoding='utf-8-sig')

        print(f"\nSUCESSO! Arquivo '{nome_arq}' gerado com {len(df)} títulos.")
    else:
        print("Nenhuma venda da LOJA encontrada no período.")


if __name__ == "__main__":
    main()
