import os
import re
import csv
import datetime
import firebirdsql

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


def create_connection_pool(pool_size):
    # Criar um pool para conexões Firebird
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


def obter_valores_custo_imposto_margem(conn, numdocumento, cd_produto, valor_final):
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

    mva_ajustado = ((1 + mva_st_original) * (1 - icms_forn)) / (1 - icms_destino) - 1

    ST = (((100 + 100 * mva_ajustado) * (1 + ipi) * (icms_destino * 100)) / 100 - 100 * icms_forn) / 100

    valor_com_st = valor_custo + (ST * valor_custo)

    valor_com_st_e_ipi = valor_com_st + (valor_custo * ipi)

    frete = valor_com_st_e_ipi * 0.1

    valor_total = valor_com_st_e_ipi + frete

    valor_impostos = valor_total - valor_custo - frete

    if valor_final is None:
        valor_margem = ""
    else:
        valor_margem = (valor_final * 0.67) - valor_custo

    return valor_custo, valor_impostos, valor_margem


def processar_pedido(pool, pedido, itens_por_pedido, clientes_dict, fones_dict, funcs_dict, cnpj):
    """ Processa um pedido e seus itens, retornando uma lista de linhas para o CSV de faturamento."""
    print(f"Processando pedido nº {pedido[0]}...")

    # Abrir conexão DENTRO da thread
    conn_local = get_connection_from_pool(pool)

    try:
        cd_ped, data_ped, nome_cli, cd_cli, cd_func, desc_ped, vl_total_ped = pedido
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
            for cd_prod, num_orig, qtd, valor_c_d, desc in itens_por_pedido[cd_ped]:
                # Normalizar campos
                num_orig = normalizar_texto(num_orig)
                desc = normalizar_texto(desc)
                classificacao_peca = classificar_item_descricao(desc)
                valor_uni = valor_c_d if valor_c_d else 0.0

                # Calcula o valor final retirando o desconto geral do pedido de forma proporcional
                vl_proporc_desc_geral = desc_ped * (valor_uni / vl_total_ped)
                valor_final = valor_uni - vl_proporc_desc_geral

                # Garantir que o valor_final é float
                if type(valor_final) != float:
                    valor_final = float(valor_final)

                # Procurar NF de compra anterior
                # Primeiro pegar data_insercao_item (data que o item foi inserido no pedido)
                data_insercao_item = get_data_insercao_item(conn_local, cd_prod, cd_ped)

                # Agora pegar numdocumento da NF COMPRA com base nessa data
                numdocumento_compra = get_numdocumento_nf_compra(conn_local, cd_prod, data_insercao_item)

                valor_custo, valor_impostos, valor_margem = "", "", ""
                if numdocumento_compra is not None:
                    # Obtém valores de custo, impostos e margem
                    valor_custo, valor_impostos, valor_margem = obter_valores_custo_imposto_margem(
                        conn_local, numdocumento_compra, cd_prod, valor_final)

                print(f"Pedido {cd_ped}, Produto {cd_prod}, Num. Original {num_orig}, Qtd {qtd}, Valor {
                      valor_final}, Desc. {desc}, Custo {valor_custo}, Impostos {valor_impostos}, Margem {valor_margem}")

                # A ordem dos campos segue esta ordem:
                # código_concessionária | numero_nota | canal | data | nome_cliente | tipo_cliente | cpf_cnpj | cep | cidade | uf |
                # ddd_telefone | telefone | chassi | modelo | ano | placa | codigo_peca | quantidade | valor | codigo_externo | classificacao_item | descricao_peca |
                # valor_custo | valor_impostos | valor_margem | classificacao_peca

                linha = [
                    cnpj, cd_ped, canal, data_ped.strftime("%Y-%m-%d"),
                    nome_cli, tipo_cli, cpf_cnpj_limpo, cep_cli, cid_cli, uf_cli, ddd, telnum, "", "", "", "",
                    num_orig, qtd, valor_final, cd_func, "", desc, valor_custo, valor_impostos, valor_margem,
                    classificacao_peca
                ]

                linha_str = [str(x) if x is not None else "" for x in linha]
                linhas.append(linha_str)
    except Exception as e:
        print(f"Erro ao processar pedido {cd_ped}: {e}")
    finally:
        release_connection_to_pool(pool, conn_local)
    return linhas


def rodar_teste_passada(pool, pedidos, max_workers, itens_por_pedido, clientes_dict, fones_dict, funcs_dict, cnpj):
    """
    Executa 1 "passada" de processamento usando 'max_workers' threads.
    Retorna (linhas_consolidadas, tempo_total, inicio, fim).
    """
    inicio = datetime.datetime.now()

    linhas_consolidadas = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(processar_pedido, pool, pedido, itens_por_pedido, clientes_dict, fones_dict, funcs_dict, cnpj)
            for pedido in pedidos
        ]
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                linhas_consolidadas.extend(res)

    fim = datetime.datetime.now()
    tempo_total = fim - inicio
    return linhas_consolidadas, tempo_total, inicio, fim


def rodar_ida_e_volta(pool, pedidos, writer, pool_size_label, itens_por_pedido, clientes_dict, fones_dict, funcs_dict, cnpj):
    """
    Faz as 4 passadas (ida 5->20, volta 20->5, ida 5->20, volta 20->5)
    usando a função rodar_teste_passada.
    Escreve os resultados no 'writer' (CSV).
    'pool_size_label' só para identificar no log qual pool_size está sendo usado.
    """

    passadas = [
        range(5, 21),        # 1ª: 5 a 20
        range(20, 4, -1),    # 2ª: 20 a 5
        range(5, 21),        # 3ª: 5 a 20
        range(20, 4, -1)     # 4ª: 20 a 5
    ]

    for idx_passada, pass_range in enumerate(passadas, start=1):
        # Logging no CSV
        writer.writerow([f"*** Passada #{idx_passada} (pool_size={pool_size_label}) ***"])

        for mw in pass_range:
            linhas, tempo_total, inicio, fim = rodar_teste_passada(pool, pedidos, mw, itens_por_pedido, clientes_dict, fones_dict, funcs_dict, cnpj)

            # Registrar info no CSV
            writer.writerow([
                f"pool_size={pool_size_label}",
                f"passada={idx_passada}",
                f"max_workers={mw}",
                f"inicio={inicio.strftime('%Y-%m-%d %H:%M:%S')}",
                f"fim={fim.strftime('%Y-%m-%d %H:%M:%S')}",
                f"tempo_total={tempo_total}"
            ])

            print(f"[Pool={pool_size_label}] Passada={idx_passada}, threads={mw}, tempo={tempo_total}")

        writer.writerow([])  # linha em branco para separar passadas

    writer.writerow([])  # mais uma linha em branco ao final de cada pool_size


def main():
    conn = get_firebird_connection()

    # 1) CONCESSÃO
    # Vamos supor que há apenas uma empresa principal, ou pegar a primeira. Caso precise filtrar,
    # o usuário não especificou qual empresa. Vamos supor que é a empresa contratada.
    # Caso haja várias, poderia precisar um filtro, mas não foi especificado. Pegaremos a empresa LOJA
    # se não for especificado. Entretanto, no faturamento citou a empresa "Loja (14.255.350/0001-03)".
    # Vamos pegar esta, pois parece ser a empresa principal.
    cnpj_loja = "14.255.350/0001-03"
    cur = conn.cursor()
    cur.execute("""
        SELECT CNPJ, INSCRICAOESTADUAL, NOMEFANTASIA, RAZAOSOCIAL, ENDERECO, NUMERO, CEP, CIDADE, UF
        FROM EMPRESA
        WHERE CNPJ = ?
    """, (cnpj_loja,))
    empresa = cur.fetchone()
    if empresa:
        (cnpj, ie, nfantasia, razao_social, ender, num, cep, cidade, uf) = empresa
    else:
        # Caso não encontre, deixa vazio
        cnpj = ie = nfantasia = razao_social = ender = num = cep = cidade = uf = ""

    # Normalizações
    cnpj = normalizar_texto(cnpj)
    ie = normalizar_texto(ie)
    nfantasia = normalizar_texto(nfantasia)
    razao_social = normalizar_texto(razao_social)
    ender = normalizar_texto(ender)
    num = normalizar_texto(num)
    cep = normalizar_texto(cep)
    cidade = normalizar_texto(cidade)
    uf = normalizar_texto(uf)

    # Cria o CSV concessão-comagro.csv
    with open("arquivos/concessao-comagro.csv", "w", encoding="utf-8", newline='') as f:
        writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_NONE, escapechar='\\')
        # Cabeçalho
        writer.writerow([
            "conta", "dms", "cnpj", "inscricao_estadual", "nome", "razao_social", "tipo_logradouro",
            "logradouro", "numero", "complemento", "cep", "cidade", "uf", "telefone_principal", "telefone_secundario"
        ])
        # Linha de dados
        # tipo_logradouro e complemento não foram informados, deixar em branco.
        # nome = NOMEFANTASIA
        writer.writerow([cnpj, "Proprio", cnpj, ie, nfantasia, razao_social, "",
                        ender, num, "", cep, cidade, uf, "7732017400", "7732017410"])

    print("Concessão processada.")

    # 2) USUÁRIOS
    # Pegar todos usuários com SETOR em ('VENDAS','ADM')
    # JOIN USUARIO CDFUNC = FUNCIONARIO CDFUNC
    # conta = empresa cnpj
    cur.execute("""
        SELECT U.SETOR, F.EMAIL, F.CDFUNC, F.CPF, F.NOME, F.FONE, F.CELULAR, F.NUMCNH
        FROM USUARIO U
        JOIN FUNCIONARIO F ON U.CDFUNC = F.CDFUNC
        JOIN EMPRESA E ON E.CNPJ = ?
        WHERE U.SETOR IN ('VENDAS', 'ADM', 'TELEVENDAS') AND
        U.HABILITADO = 'S'
    """, (cnpj_loja,))
    usuarios = cur.fetchall()

    with open("arquivos/usuarios-comagro.csv", "w", encoding="utf-8", newline='') as f:
        writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_NONE, escapechar='\\')
        # Cabeçalho
        writer.writerow(["conta", "email_login", "codigo_externo", "cpf",
                        "nome", "telefone", "celular", "canal", "papel"])
        for (setor, email, cd_func, cpf, nome, fone, celular, num_cnh) in usuarios:
            email = normalizar_texto(email)
            cpf = normalizar_texto(cpf)
            nome = normalizar_texto(nome)
            fone = limpar_telefone(fone)
            celular = limpar_telefone(celular)

            if num_cnh:
                num_cnh = normalizar_texto(num_cnh)
            else:
                num_cnh = "BALCAO,TELEPECAS"  # Se não tiver posição definida, assumir as duas.

            # papel
            if setor == 'ADM':
                papel = 'GERENTE'
            elif setor == 'VENDAS' or setor == 'TELEVENDAS':
                papel = 'VENDEDOR'
            else:
                papel = ''

            writer.writerow([cnpj, email, cd_func, cpf, nome, fone, celular, num_cnh, papel])

    print("Usuários processados.")

    # 3) FATURAMENTO
    # Empresa Loja (CNPJ = 14.255.350/0001-03)
    # Data: de 2 anos atrás até hoje
    # Para cada item de cada pedido, uma linha.

    # start_date = (datetime.datetime.now() - datetime.timedelta(days=2*365)).strftime('%Y-%m-%d')
    start_date = (datetime.datetime.now() - datetime.timedelta(days=10)).strftime('%Y-%m-%d')
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')

    # Vamos buscar pedidos desta empresa e deste período
    # Assumindo: PEDIDOVENDA possui CDPEDIDOVENDA, DATA, NOMECLIENTE, CDCLIENTE, CDFUNC.
    # ITENSPEDIDOVENDA: CDPEDIDOVENDA, CDPRODUTO, NUMORIGINAL, QUANTIDADE, VALORUNITARIOCDESC, DESCRICAO
    # FUNCIONARIO: CDFUNC, NUMCNH
    # CLIENTE: CDCLIENTE, CPF_CNPJ, CEP, CIDADE, ESTADO
    # FONE: CDCLIENTE, FONE (uma entrada? se houver várias, pegar uma. Não está claro, vamos pegar a primeira.)
    # NOTA: Para cada item, buscaremos a NF de compra mais recente. Se não houver, campos vazios.
    # Canal: se NUMCNH = "TELEPECAS", então canal = "TELEP", senão canal = NUMCNH

    # Primeiro pegamos todos PEDIDOVENDA da empresa neste período
    cur.execute("""
        SELECT P.CDPEDIDOVENDA, P.DATA, P.NOMECLIENTE, P.CDCLIENTE, P.CDFUNC, P.DESCONTO, P.VALORTOTAL
        FROM PEDIDOVENDA P
        JOIN EMPRESA E ON E.CNPJ = ?
        WHERE P.DATA BETWEEN ? AND ? AND
        P.EFETIVADO = 'S'
    """, (cnpj_loja, start_date, end_date))
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

    # Precisamos dados dos clientes dos pedidos
    cd_clientes = set(p[3] for p in pedidos if p[3] is not None)
    clientes_dict = {}
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
    fones_dict = {}
    if cd_clientes:
        sql_fones = f"""
            SELECT CDCLIENTE, FONE
            FROM FONE
            WHERE CDCLIENTE IN ({format_strings})
        """
        cur.execute(sql_fones, tuple(cd_clientes))
        for (cd_cli, fone) in cur.fetchall():
            if cd_cli not in fones_dict:  # Pega apenas o primeiro
                fones_dict[cd_cli] = normalizar_texto(fone)

    # Dados dos funcionários (para canal)
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

    # 2) Definir os diferentes pool_sizes
    lista_pool_sizes = [5, 10, 15, 20]

    # Montar o CSV de faturamento
    # Sem cabeçalho, sem rodapé, sem espaço extra
    with open("arquivos/faturamento-comagro.csv", "w", encoding="utf-8", newline='') as f:
        writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_NONE, escapechar='\\')
        # Sem cabeçalho

        writer.writerow(["Teste com 4 passadas (ida/volta) em cada pool_size."])

        # 4) Loop para cada pool_size
        for ps in lista_pool_sizes:
            # 4.1) Criar o pool
            pool = create_connection_pool(pool_size=ps)

            # 4.2) Executar as passadas (ida e volta)
            rodar_ida_e_volta(pool, pedidos, writer, ps, itens_por_pedido, clientes_dict, fones_dict, funcs_dict, cnpj)

            # 4.3) Fechar as conexões do pool
            while not pool.empty():
                c = pool.get()
                c.close()

        print("Faturamento processado.")

    print("Processamento concluído.")


if __name__ == "__main__":
    main()
