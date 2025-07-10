import os
import re
import csv
import datetime
import argparse
import firebirdsql
import pysftp

from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from queue import Queue

# Carregar variáveis de ambiente
load_dotenv()


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


# Função fictícia de SFTP (dados incompletos)
def enviar_arquivo_sftp(file_path):
    """
    Função para ilustrar uma conexão SFTP.
    Dados de host, user, password etc. ainda não estão definidos.
    """

    cnopts = pysftp.CnOpts()

    # Carrega o arquivo 'my_known_hosts' com chaves conhecidas
    cnopts.hostkeys.load('my_known_hosts')

    # Configuração do servidor SFTP
    sftp_host = os.getenv('SFTP_HOST')
    sftp_user = os.getenv('SFTP_USER')
    sftp_pass = os.getenv('SFTP_PASSWORD')
    remote_dir = '/workarea'

    with pysftp.Connection(host=sftp_host, username=sftp_user, password=sftp_pass, cnopts=cnopts) as sftp:
        with sftp.cd(remote_dir):
            sftp.put(file_path)  # envia o arquivo
            print("Arquivo enviado com sucesso.")


def create_connection_pool(pool_size=20):
    """
    Cria pool de conexões Firebird com pool_size=20 (fixo).
    """
    pool = Queue(maxsize=pool_size)

    for _ in range(pool_size):
        conn = get_firebird_connection()
        pool.put(conn)
    return pool


def get_connection_from_pool(pool):
    # Fica bloqueado até ter uma conexão disponível no pool
    conn = pool.get()
    return conn


def release_connection_to_pool(pool, conn):
    # Devolve a conexão ao pool
    pool.put(conn)


def dividir_em_blocos(lst, grp_size):
    # Dividir a lista de IDs em blocos menores
    for i in range(0, len(lst), grp_size):
        yield lst[i:i + grp_size]


def normalizar_texto(texto):
    """Normaliza o texto conforme requerido
    - Substituir ";" interno por ","
    - Caso seja necessário, pode-se adicionar mais correções aqui.
    """
    if texto is None:
        return ""
    # Substituir ";" por "," dentro do texto (exceto o separador, mas esse é só na hora do CSV)
    texto = texto.replace(";", ",")
    return texto.strip()


def limpar_telefone(telefone):
    """Remove espaços em branco, caracteres alfabéticos e deixa apenas números."""
    if telefone is None:
        return ""
    tel_limpo = re.sub(r'\D', '', telefone)  # remove tudo que não for dígito
    return tel_limpo


def extrair_ddd_telefone(telefone):
    """
    Extrair DDD (2 primeiros dígitos) e o restante do telefone.
    Caso não seja possível extrair, retornar ddd vazio e telefone vazio.
    """
    tel = limpar_telefone(telefone)
    if len(tel) >= 3:
        ddd = tel[:2]
        numero = tel[2:]
        return ddd, numero
    return "", ""


def classificar_item_descricao(descricao):
    """Se a descrição contiver 'ORIGINAL', 'GENUINO' ou 'ORIG', retorna 1.
    Caso contrário, retorna 5.
    Considerar que pode haver caracteres especiais. Busca as palavras em modo case-insensitive."""
    if descricao is None:
        return 5
    desc_up = descricao.upper()
    if "ORIGINAL" in desc_up or "GENUINO" in desc_up or "ORIG" in desc_up:
        return 1
    return 5


def remover_caracteres_nao_numericos(texto):
    """Remove todos os caracteres não numéricos."""
    if texto is None:
        return ""
    return re.sub(r'\D', '', texto)


def determinar_tipo_cliente(cpf_cnpj_limpo):
    """Se tiver 11 dígitos => 'F' (física), se tiver 14 dígitos => 'J' (jurídica)."""
    length = len(cpf_cnpj_limpo)
    if length == 11:
        return 'F'  # Pessoa Física
    elif length == 14:
        return 'J'  # Pessoa Jurídica
    return ''  # caso não seja nem 11 nem 14


def normalizar_numero(valor):
    """
    Substitui o ponto decimal por vírgula, 
    arredonda para duas casas decimais se for numérico.
    Se 'valor' não for conversível em float, 
    apenas faz 'replace' de '.' por ','.
    """
    if not valor:
        return ""

    try:
        # Tenta converter em float
        f_val = float(valor)
        # Arredonda para duas casas decimais
        f_val = round(f_val, 2)
        # Formata com 2 casas decimais e troca '.' por ','
        val_str = f"{f_val:.2f}".replace('.', ',')
        return val_str
    except ValueError:
        # Se não der para converter em float,
        # apenas troca '.' por ',' no texto original
        val_str = str(valor).replace('.', ',')
        return val_str


def get_data_insercao_item(conn, cd_produto, cd_ped):
    """
    Pega a data de inserção do item a partir das tabelas HISTORICOPRODUTOX
    onde TIPO='PEDIDO', NUMDOCUMENTO=cd_ped, CDPRODUTO=cd_produto.
    Pegaremos a primeira data encontrada (a query ordena por DATA).
    """
    for i in range(1, 11):
        tabela = f"HISTORICOPRODUTO{i}"
        sql = f"""
            SELECT FIRST 1 DATA
            FROM {tabela}
            WHERE TIPO = 'PEDIDO'
              AND CDPRODUTO = ?
              AND NUMDOCUMENTO = ?
            ORDER BY DATA
        """
        cur = conn.cursor()
        cur.execute(sql, (cd_produto, cd_ped))
        row = cur.fetchone()
        if row:
            return row[0]
    return None


def get_numdocumento_nf_compra(conn, cd_produto, data_insercao_item):
    """
    Pega o número da NF de compra mais recente (DATA <= data_insercao_item)
    a partir das tabelas HISTORICOPRODUTOX onde TIPO='NF COMPRA'.
    """
    if data_insercao_item is None:
        return None
    for i in range(1, 11):
        tabela = f"HISTORICOPRODUTO{i}"
        sql = f"""
            SELECT FIRST 1 NUMDOCUMENTO
            FROM {tabela}
            WHERE TIPO = 'NF COMPRA'
              AND CDPRODUTO = ?
              AND DATA <= ?
            ORDER BY DATA DESC
        """
        cur = conn.cursor()
        cur.execute(sql, (cd_produto, data_insercao_item))
        row = cur.fetchone()
        if row:
            return row[0]
    return None


def recuperar_historico_numdocumento(conn, cd_produto, data_venda):
    """
    Precisa buscar nas tabelas HISTORICOPRODUTO1 a HISTORICOPRODUTO10 
    o registro mais recente anterior ou igual à data da venda com TIPO = 'NF COMPRA'.
    Como não sabemos a estrutura exata, tentaremos todas de 1 a 10.
    """

    # Queremos a data anterior OU IGUAL mais próxima. Vamos tentar <= data_venda.
    # De acordo com a descrição, "a data anterior mais próxima da data da venda".
    # Se não achar exatamente menor, pode igualar a data da venda.

    # Tentaremos do mais próximo da venda:
    # SELECT NUMDOCUMENTO, DATA FROM HISTORICOPRODUTOX WHERE TIPO='NF COMPRA' AND CDPRODUTO=? AND DATA<=? ORDER BY DATA DESC ROWS 1
    # Caso não encontre em nenhuma das 10 tabelas, retorna None.

    for i in range(1, 11):
        tabela = f"HISTORICOPRODUTO{i}"
        sql = f"""
            SELECT NUMDOCUMENTO, DATA
            FROM {tabela}
            WHERE TIPO = 'NF COMPRA'
              AND CDPRODUTO = ?
              AND DATA <= ?
            ORDER BY DATA DESC
            ROWS 1
        """
        cur = conn.cursor()
        cur.execute(sql, (cd_produto, data_venda))
        row = cur.fetchone()
        if row:
            return row[0]  # NUMDOCUMENTO da NF de compra encontrada
    return None


def obter_valores_custo_imposto_margem(conn, numdocumento, cd_produto, valor_final, qtd):
    """
    Para obter valor_custo, valor_imposto, valor_margem:
    1) Achar NOTACOMPRA.CDNOTACOMPRA usando NOTACOMPRA.NUMNOTA = numdocumento
    2) Achar ITENSNOTACOMPRA onde ITENSNOTACOMPRA.CDNOTACOMPRA = NOTACOMPRA.CDNOTACOMPRA e ITENSNOTACOMPRA.CDPRODUTO = cd_produto
    3) valor_custo = ITENSNOTACOMPRA.VALORUNITARIO
    4) ipi = ITENSNOTACOMPRA.IPI/100
    5) icms_forn = ITENSNOTACOMPRA.ICMS/100
    """
    if numdocumento is None:
        return "", "", ""

    cur = conn.cursor()
    cur.execute("SELECT CDNOTACOMPRA FROM NOTACOMPRA WHERE NUMNOTA = ?", (numdocumento,))
    nota = cur.fetchone()
    if not nota:
        return "", "", ""
    cdnotacompra = nota[0]

    cur.execute("""
        SELECT VALORUNITARIO, IPI, ICMS
        FROM ITENSNOTACOMPRA
        WHERE CDNOTACOMPRA = ? AND CDPRODUTO = ?
    """, (cdnotacompra, cd_produto))
    item_nc = cur.fetchone()
    if not item_nc:
        return "", "", ""

    valor_custo = float(item_nc[0])
    ipi = (float(item_nc[1]) or 0) / 100.0
    icms_forn = (float(item_nc[2]) or 0) / 100.0

    # Cálculo revisado
    mva_st_original = 71.78 / 100
    icms_destino = 20.5 / 100

    mva_ajustado = (((1 + mva_st_original) * (1 - icms_forn) / (1 - icms_destino)) - 1) * 100
    ST = (((((100 + mva_ajustado) * (1 + ipi)) * (icms_destino * 100)) / 100) - 100 * icms_forn) / 100
    valor_com_st = valor_custo + (ST * valor_custo)
    valor_com_st_e_ipi = valor_com_st + (valor_custo * ipi)
    frete = valor_com_st_e_ipi * 0.1
    valor_total = valor_com_st_e_ipi + frete
    valor_impostos = valor_total - valor_custo - frete

    # Multiplicar pela quantidade para obter os valores totais da linha
    valor_custo_total = valor_custo * qtd
    valor_impostos_total = valor_impostos * qtd

    if valor_final is None:
        valor_margem = ""
    else:
        valor_margem = (valor_final * 0.67) - valor_custo_total

    return valor_custo_total, valor_impostos_total, valor_margem


def processar_pedido(pool, pedido, itens_por_pedido, clientes_dict, fones_dict, funcs_dict, cnpj):
    """ Processa um pedido e seus itens, retornando uma lista de linhas para o CSV de faturamento."""
    # Abrir conexão DENTRO da thread
    cd_ped, data_ped, nome_cli, cd_cli, cd_func, desc_ped, vl_total_ped = pedido

    conn_local = get_connection_from_pool(pool)
    linhas = []

    try:
        # Dados do cliente
        if cd_cli in clientes_dict:
            cpf_cnpj_cli = clientes_dict[cd_cli]['cpf_cnpj']
            cep_cli = clientes_dict[cd_cli]['cep']
            cid_cli = clientes_dict[cd_cli]['cidade']
            uf_cli = clientes_dict[cd_cli]['uf']
        else:
            cpf_cnpj_cli = cep_cli = cid_cli = uf_cli = ""

        # Pegar CNPJ e tipo do cliente
        cpf_cnpj_limpo = remover_caracteres_nao_numericos(cpf_cnpj_cli)
        tipo_cli = determinar_tipo_cliente(cpf_cnpj_limpo)

        # Dados do telefone do cliente
        fone_cli = fones_dict.get(cd_cli, "")
        ddd, telnum = extrair_ddd_telefone(fone_cli)

        # Canal de venda
        canal = funcs_dict.get(cd_func, "")

        # Nome do cliente
        nome_cli = normalizar_texto(nome_cli)

        linhas = []
        # Para cada item do pedido
        if cd_ped in itens_por_pedido:
            for (cd_prod, num_orig, qtd, valor_c_d, desc) in itens_por_pedido[cd_ped]:
                # Normalizar campos
                num_orig = normalizar_texto(num_orig)
                desc = normalizar_texto(desc)
                classificacao_peca = classificar_item_descricao(desc)
                valor_uni = valor_c_d if valor_c_d else 0.0

                # Calcula o valor final retirando o desconto geral do pedido de forma proporcional
                vl_proporc_desc_geral = desc_ped * (valor_uni / vl_total_ped)

                # Seta a quantidade como float
                try:
                    qtd = float(qtd)
                except ValueError:
                    print(f"Quantidade inválida para o pedido {cd_ped}, produto {cd_prod}: {qtd}")
                    continue

                # Garantir que o valor_final é float
                valor_final = float(valor_uni - vl_proporc_desc_geral) * qtd

                # Procurar NF de compra anterior
                # Primeiro pegar data_insercao_item (data que o item foi inserido no pedido)
                data_insercao_item = get_data_insercao_item(conn_local, cd_prod, cd_ped)

                # Agora pegar numdocumento da NF COMPRA com base nessa data
                numdocumento_compra = get_numdocumento_nf_compra(conn_local, cd_prod, data_insercao_item)

                valor_custo, valor_impostos, valor_margem = "", "", ""
                if numdocumento_compra is not None:
                    # Obtém valores de custo, impostos e margem
                    custo, imp, marg = obter_valores_custo_imposto_margem(
                        conn_local, numdocumento_compra, cd_prod, valor_final, qtd)

                    # Converte para string e troca '.' por ','
                    valor_custo = normalizar_numero(custo)
                    valor_impostos = normalizar_numero(imp)
                    valor_margem = normalizar_numero(marg)

                # Transforma o valor_final e a quantidade em string e troca '.' por ','
                valor_final = normalizar_numero(valor_final)
                qtd = normalizar_numero(qtd)

                print(f"Pedido {cd_ped}, Produto {cd_prod}, Num. Original {num_orig}, Qtd {qtd}, Valor {
                      valor_final}, Desc. {desc}, Custo {valor_custo}, Impostos {valor_impostos}, Margem {valor_margem}")

                # A ordem dos campos segue esta ordem:
                # código_concessionária | numero_nota | canal | data | nome_cliente | tipo_cliente | cpf_cnpj | cep | cidade | uf |
                # ddd_telefone | telefone | chassi | modelo | ano | placa | codigo_peca | quantidade | valor | codigo_externo | classificacao_item | descricao_peca |
                # valor_custo | valor_impostos | valor_margem | classificacao_peca

                linha = [
                    cnpj,  # código_concessionária
                    cd_ped,  # numero_nota
                    canal,  # canal
                    data_ped.strftime("%d/%m/%Y"),  # data no formato dd/mm/aaaa
                    nome_cli,  # nome_cliente
                    tipo_cli,  # tipo_cliente
                    cpf_cnpj_limpo,  # cpf_cnpj
                    cep_cli,  # cep
                    cid_cli,  # cidade
                    uf_cli,  # uf
                    ddd,  # ddd_telefone
                    telnum,  # telefone
                    "",  # chassi
                    "",  # modelo
                    "",  # ano
                    "",  # placa
                    num_orig,  # codigo_peca
                    qtd,  # quantidade
                    valor_final,  # valor
                    cd_func,  # codigo_externo
                    "",  # classificacao_item
                    desc,  # descricao_peca
                    valor_custo,  # valor_custo
                    valor_impostos,  # valor_impostos
                    valor_margem,  # valor_margem
                    classificacao_peca  # classificacao_peca
                ]

                linha_str = [str(x) if x is not None else "" for x in linha]
                linhas.append(linha_str)
    except Exception as e:
        print(f"Erro ao processar pedido {cd_ped}: {e}")
    finally:
        release_connection_to_pool(pool, conn_local)
    return linhas


def main():
    # Argumentos de linha de comando
    parser = argparse.ArgumentParser()
    parser.add_argument("--poolsize", type=int, default=5, help="Tamanho do pool de conexões Firebird")
    parser.add_argument("--threads", type=int, default=5, help="Número de threads (max_workers)")
    parser.add_argument("--passada", type=int, default=1, help="Qual passada é? (só para log)")
    args = parser.parse_args()

    max_workers = args.threads
    ps = args.poolsize
    passada = args.passada

    print(f"== Iniciando FATURAMENTO: pool_size={ps}, threads={max_workers}, passada={passada} ==\n")

    # Conexão com Firebird
    conn = get_firebird_connection()

    cnpj_loja = "14255350000103"
    cur = conn.cursor()

    cnpj = normalizar_texto(cnpj_loja)

    # 3) FATURAMENTO
    # Empresa Loja (CNPJ = 14255350000103)
    # Data: todo dia
    # Para cada item de cada pedido, uma linha.

    # Vamos buscar pedidos desta empresa e deste período
    # PEDIDOVENDA possui CDPEDIDOVENDA, DATA, NOMECLIENTE, CDCLIENTE, CDFUNC.
    # ITENSPEDIDOVENDA: CDPEDIDOVENDA, CDPRODUTO, NUMORIGINAL, QUANTIDADE, VALORUNITARIOCDESC, DESCRICAO
    # FUNCIONARIO: CDFUNC, NUMCNH
    # CLIENTE: CDCLIENTE, CPF_CNPJ, CEP, CIDADE, ESTADO
    # FONE: CDCLIENTE, FONE (uma entrada? se houver várias, pegar uma. Não está claro, vamos pegar a primeira.)
    # NOTA: Para cada item, buscaremos a NF de compra mais recente. Se não houver, campos vazios.
    # Canal: se NUMCNH = "TELEPECAS", então canal = "TELEP", senão canal = NUMCNH

    # Data de início e fim
    start_date = '2023-02-18'
    # start_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')

    # Chunk de 30 dias para não estourar a memória
    chunk_days = 30

    # Variáveis para usar com o chunk de 30 dias
    current_start = start_date
    current_start = datetime.datetime.strptime(current_start, "%Y-%m-%d")
    end_full = end_date
    end_full = datetime.datetime.strptime(end_full, "%Y-%m-%d")

    # Definindo pool_size=20 e max_workers=5
    pool_size = 20
    max_workers = 5

    date_start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").strftime("%d%m%y")
    date_end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").strftime("%d%m%y")

    if end_date == start_date:
        # Caminho do CVS
        csv_path = f"./arquivos/faturamento_diario_{date_start_date}.csv"
    else:
        # Caminho do CVS
        csv_path = f"./arquivos/faturamento_retroativo_{date_start_date}_{date_end_date}.csv"

    # Criando ou limpando o arquivo CSV no primeiro chunk
    if current_start == start_date:
        with open(csv_path, "w", encoding="utf-8", newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter=';', quoting=csv.QUOTE_NONE, escapechar='\\')

    # Loop principal para processar os chunks de 30 dias até a data final
    while current_start <= end_full:
        # Definindo o chunk de 30 dias
        current_end = current_start + datetime.timedelta(days=chunk_days - 1)

        # Se o chunk de 30 dias ultrapassar a data final, ajustar para a data final
        if current_end > end_full:
            current_end = end_full

        print(f"\n=== Processando chunk: {current_start} até {current_end} ===")

        # Conexão com Firebird
        conn = get_firebird_connection()
        cur = conn.cursor()

        # CNPJ da empresa
        cnpj_loja = "14.255.350/0001-03"
        cnpj = normalizar_texto(cnpj_loja)

        # Primeiro pegamos todos PEDIDOVENDA da empresa neste período
        cur.execute("""
            SELECT P.CDPEDIDOVENDA, P.DATA, P.NOMECLIENTE, P.CDCLIENTE, P.CDFUNC, P.DESCONTO, P.VALORTOTAL
            FROM PEDIDOVENDA P
            JOIN EMPRESA E ON E.CNPJ = ?
            WHERE P.DATA BETWEEN ? AND ? AND
            P.EFETIVADO = 'S'
        """, (cnpj_loja, current_start, current_end))
        pedidos = cur.fetchall()

        # Vamos construir um dicionário de pedidos -> itens
        # Depois buscar os dados auxiliares
        pedidos_ids = [p[0] for p in pedidos]
        itens = []
        if pedidos_ids:
            grp_size = 1499  # Limite no Firebird
            # Obter itens
            for grupo in dividir_em_blocos(pedidos_ids, grp_size):
                format_strings = ','.join(['?'] * len(grupo))
                sql_itens = f"""
                    SELECT I.CDPEDIDOVENDA, I.CDPRODUTO, I.NUMORIGINAL, I.QUANTIDADE, I.VALORUNITARIOCDESC, I.DESCRICAO
                    FROM ITENSPEDIDOVENDA I
                    WHERE I.CDPEDIDOVENDA IN ({format_strings})
                """
                cur.execute(sql_itens, tuple(grupo))
                itens.extend(cur.fetchall())

        # Organizar itens por pedido
        itens_por_pedido = {}
        for (cd_ped, cd_prod, num_orig, qtd, valorcdesc, desc) in itens:
            if cd_ped not in itens_por_pedido:
                itens_por_pedido[cd_ped] = []
            itens_por_pedido[cd_ped].append((cd_prod, num_orig, qtd, valorcdesc, desc))

        # Dados dos clientes de todos os pedidos do período
        cd_clientes = set(p[3] for p in pedidos if p[3] is not None)
        clientes_dict = {}
        fones_dict = {}
        if cd_clientes:
            format_strings = ','.join(['?']*len(cd_clientes))  # IN (?, ?, ...)
            sql_clientes = f"""
                SELECT CDCLIENTE, CPF_CNPJ, CEP, CIDADE, ESTADO
                FROM CLIENTE
                WHERE CDCLIENTE IN ({format_strings})
            """
            cur.execute(sql_clientes, tuple(cd_clientes))
            for row in cur.fetchall():
                (cd_cli, cpf_cnpj_cli, cep_cli, cid_cli, uf_cli) = row
                clientes_dict[cd_cli] = {
                    'cpf_cnpj': normalizar_texto(cpf_cnpj_cli),
                    'cep': normalizar_texto(cep_cli),
                    'cidade': normalizar_texto(cid_cli),
                    'uf': normalizar_texto(uf_cli)
                }

            # Dados de telefone do cliente, pegamos apenas o primeiro
            sql_fones = f"""
                SELECT CDCLIENTE, FONE
                FROM FONE
                WHERE CDCLIENTE IN ({format_strings})
            """
            cur.execute(sql_fones, tuple(cd_clientes))
            for (cd_cli, fone) in cur.fetchall():
                if cd_cli not in fones_dict:  # Pega apenas o primeiro
                    fones_dict[cd_cli] = normalizar_texto(fone)

        # Dados dos funcionários (para canal de vendas)
        cd_funcs = set(p[4] for p in pedidos if p[4] is not None)
        funcs_dict = {}
        if cd_funcs:
            format_strings = ','.join(['?']*len(cd_funcs))
            sql_func = f"""
                SELECT CDFUNC, NUMCNH
                FROM FUNCIONARIO
                WHERE CDFUNC IN ({format_strings})
            """
            cur.execute(sql_func, tuple(cd_funcs))
            for (cdf, ncnh) in cur.fetchall():
                ncnh = normalizar_texto(ncnh)
                canal = "TELEP" if ncnh.upper() == "TELEPECAS" else ncnh.upper()
                funcs_dict[cdf] = canal

        conn.close()

        # Limpar o CNPJ da loja
        cnpj = remover_caracteres_nao_numericos(cnpj)

        # Criando a pool de conexões sempre com pool_size=20
        pool = create_connection_pool(pool_size=pool_size)

        # Processando os pedidos sempre com max_workers=5
        linhas_csv = []
        start_chunk = datetime.datetime.now()  # Início do chunk
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for pedido in pedidos:
                # Processar cada pedido em uma thread
                fut = executor.submit(
                    processar_pedido, pool, pedido,
                    itens_por_pedido, clientes_dict, fones_dict, funcs_dict,
                    cnpj
                )
                futures.append(fut)
            for fut in as_completed(futures):
                # Agregar as linhas de cada pedido no CSV
                res = fut.result()
                if res:
                    linhas_csv.extend(res)

        end_chunk = datetime.datetime.now()  # Fim do chunk
        elapsed = end_chunk - start_chunk  # Tempo de processamento do chunk
        print(f"Chunk {current_start} - {current_end} finalizado em {elapsed}.")

        # 4.3) Fechar as conexões do pool
        while not pool.empty():
            c = pool.get()
            c.close()

        print(f"Linhas CSV: {len(linhas_csv)}")

        # Escrever as linhas no CSV
        with open(csv_path, "a", encoding="utf-8", newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter=';', quoting=csv.QUOTE_NONE, escapechar='\\')
            for row in linhas_csv:
                writer.writerow(row)

        # Avançar para o próximo chunk
        current_start = current_end + datetime.timedelta(days=1)

    enviar_arquivo_sftp(csv_path)
    print("Processamento concluído.")


if __name__ == "__main__":
    main()
