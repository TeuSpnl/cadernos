import os
import re
import csv
import datetime
import argparse
import firebirdsql
import pysftp

from collections import defaultdict
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


def pre_cache_historico_produtos(conn, produtos_ids, end_date):
    """
    Busca o histórico de produtos consultando cada uma das 10 tabelas de histórico
    separadamente e em blocos para respeitar os limites do Firebird 2.1.
    """
    if not produtos_ids:
        return defaultdict(list)

    historico_por_produto = defaultdict(list)
    cur = conn.cursor()

    # Limite seguro de parâmetros para a cláusula IN. Limite é 1500
    chunk_size = 1450

    # 1. Itera sobre cada uma das 10 tabelas de histórico
    for i in range(1, 11):
        tabela = f"HISTORICOPRODUTO{i}"

        # 2. Divide a lista total de produtos em blocos menores (chunks)
        for grupo_produtos in dividir_em_blocos(list(produtos_ids), chunk_size):
            format_strings = ','.join(['?'] * len(grupo_produtos))
            sql = f"""
                SELECT CDPRODUTO, DATA, NUMDOCUMENTO, TIPO
                FROM {tabela}
                WHERE 
                TIPO IN ('PEDIDO', 'NF COMPRA') AND 
                CDPRODUTO IN ({format_strings})
                AND DATA <= ?
            """

            try:
                params = tuple(grupo_produtos) + (end_date,)
                cur.execute(sql, params)
                for cd_produto, data, num_documento, tipo in cur.fetchall():
                    historico_por_produto[cd_produto].append({
                        'data': data,
                        'numdoc': num_documento,
                        'tipo': tipo
                    })
            except Exception as e:
                print(f"Aviso: Erro ao consultar a tabela {tabela}. Erro: {e}")
                # Continua para a próxima tabela/bloco
                continue

    return historico_por_produto


def pre_cache_dados_compra(conn, num_documentos_compra):
    """
    Busca de uma só vez os dados de custo de todas as NFs de compra encontradas.
    """
    if not num_documentos_compra:
        return {}

    dados_compra = {}

    # Usamos blocos para não exceder o limite de parâmetros do 'IN'
    for grupo_docs in dividir_em_blocos(list(num_documentos_compra), 500):
        format_strings = ','.join(['?'] * len(grupo_docs))
        sql = f"""
            SELECT NC.NUMNOTA, INC.CDPRODUTO, INC.VALORUNITARIO, INC.IPI, INC.ICMS
            FROM NOTACOMPRA NC
            JOIN ITENSNOTACOMPRA INC ON NC.CDNOTACOMPRA = INC.CDNOTACOMPRA
            WHERE NC.NUMNOTA IN ({format_strings})
        """
        cur = conn.cursor()
        cur.execute(sql, tuple(grupo_docs))

        for num_nota, cd_produto, valor_unit, ipi, icms in cur.fetchall():
            # Chaveia por (número da nota, código do produto) para busca rápida
            dados_compra[(num_nota, cd_produto)] = {
                'valor_unitario': float(valor_unit or 0),
                'ipi': float(ipi or 0),
                'icms': float(icms or 0)
            }

    return dados_compra


def calcular_valores_finais(valor_custo, ipi_perc, icms_forn_perc, valor_final_venda, qtd):
    """
    Função pura de cálculo, sem acesso ao banco. Recebe os valores e retorna os resultados.
    """
    ipi = ipi_perc / 100.0
    icms_forn = icms_forn_perc / 100.0

    # Cálculo revisado
    mva_st_original = 71.78 / 100
    icms_destino = 20.5 / 100

    # Evita divisão por zero
    if (1 - icms_destino) == 0:
        mva_ajustado = 0
    else:
        mva_ajustado = (((1 + mva_st_original) * (1 - icms_forn) / (1 - icms_destino)) - 1) * 100

    ST = (((((100 + mva_ajustado) * (1 + ipi)) * (icms_destino * 100)) / 100) - 100 * icms_forn) / 100
    valor_com_st = valor_custo + (ST * valor_custo)
    valor_com_st_e_ipi = valor_com_st + (valor_custo * ipi)
    frete = valor_com_st_e_ipi * 0.1
    valor_total = valor_com_st_e_ipi + frete
    valor_impostos = valor_total - valor_custo - frete

    valor_custo_total = valor_custo * qtd
    valor_impostos_total = valor_impostos * qtd

    if valor_final_venda is None:
        valor_margem = ""
    else:
        valor_margem = (valor_final_venda * 0.67) - valor_custo_total

    return valor_custo_total, valor_impostos_total, valor_margem


def processar_pedido_otimizado(pedido, itens_do_pedido, clientes_dict, fones_dict, funcs_dict, cnpj, item_custo_info):
    """
    Processa um pedido e seus itens usando os dados pré-carregados (em cache).
    NÃO faz nenhuma consulta ao banco de dados.
    """
    cd_ped, data_ped, nome_cli, cd_cli, cd_func, desc_ped, vl_total_ped = pedido
    linhas = []

    try:
        cliente_info = clientes_dict.get(cd_cli, {})
        cpf_cnpj_cli = cliente_info.get('cpf_cnpj', "")
        cep_cli = cliente_info.get('cep', "")
        cid_cli = cliente_info.get('cidade', "")
        uf_cli = cliente_info.get('uf', "")

        cpf_cnpj_limpo = remover_caracteres_nao_numericos(cpf_cnpj_cli)
        tipo_cli = determinar_tipo_cliente(cpf_cnpj_limpo)

        fone_cli = fones_dict.get(cd_cli, "")
        ddd, telnum = extrair_ddd_telefone(fone_cli)
        canal = funcs_dict.get(cd_func, "")
        nome_cli = normalizar_texto(nome_cli)

        for (cd_prod, num_orig, qtd, valor_c_d, desc) in itens_do_pedido:
            num_orig = normalizar_texto(num_orig)
            desc = normalizar_texto(desc)
            classificacao_peca = classificar_item_descricao(desc)
            valor_uni = valor_c_d if valor_c_d else 0.0

            try:
                qtd_float = float(qtd)
            except (ValueError, TypeError):
                print(f"Quantidade inválida para o pedido {cd_ped}, produto {cd_prod}: {qtd}")
                continue

            # Cálculo proporcional do desconto
            vl_proporc_desc_geral = 0
            if vl_total_ped and vl_total_ped > 0:
                vl_proporc_desc_geral = desc_ped * (valor_uni / vl_total_ped)

            valor_final = float(valor_uni - vl_proporc_desc_geral) * qtd_float

            valor_custo, valor_impostos, valor_margem = "", "", ""

            # Busca as informações de custo no dicionário pré-carregado
            custo_info = item_custo_info.get((cd_ped, cd_prod))

            if custo_info:
                custo, imp, marg = calcular_valores_finais(
                    custo_info['valor_unitario'],
                    custo_info['ipi'],
                    custo_info['icms'],
                    valor_final,
                    qtd_float
                )
                valor_custo = normalizar_numero(custo)
                valor_impostos = normalizar_numero(imp)
                valor_margem = normalizar_numero(marg)

            # Formatação final
            valor_final_str = normalizar_numero(valor_final)
            qtd_str = normalizar_numero(qtd_float)

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

            linhas.append([str(x) if x is not None else "" for x in linha])
    except Exception as e:
        print(f"Erro ao processar pedido {cd_ped} em memória: {e}")

    return linhas


def main():
    # Argumentos de linha de comando
    parser = argparse.ArgumentParser()
    parser.add_argument("--poolsize", type=int, default=5, help="Tamanho do pool de conexões Firebird")
    parser.add_argument("--threads", type=int, default=5, help="Número de threads (threads)")
    parser.add_argument("--passada", type=int, default=1, help="Qual passada é? (só para log)")
    args = parser.parse_args()

    threads = args.threads
    ps = args.poolsize
    passada = args.passada

    # Pool de conexões não é mais necessário para o processamento em threads,
    # mas pode ser mantido para as buscas iniciais se forem paralelizadas no futuro.
    # Por enquanto, uma conexão principal é suficiente.

    print(f"== Iniciando FATURAMENTO: threads={threads} ==\n")

    # Conexão com Firebird
    conn = get_firebird_connection()
    cur = conn.cursor()

    cnpj_loja = "14.255.350/0001-03"
    cnpj = normalizar_texto(cnpj_loja)

    # 3) FATURAMENTO

    # Data de início e fim
    start_date = '2023-02-18'
    # start_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    # Chunk de 30 dias para não estourar a memória
    chunk_days = 30

    # Variáveis para usar com o chunk de 30 dias
    current_start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_full_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    if end_date == start_date:
        # Caminho do CVS
        csv_path = f"./arquivos/faturamento_diario_{current_start_dt.strftime('%d%m%y')}.csv"
    else:
        # Caminho do CVS
        csv_path = f"./arquivos/faturamento_retroativo_{current_start_dt.strftime('%d%m%y')}_{end_full_dt.strftime('%d%m%y')}.csv"

    with open(csv_path, "w", encoding="utf-8", newline='') as f:
        # Apenas cria/limpa o arquivo
        pass

    # Loop principal para processar os chunks de 30 dias até a data final
    while current_start_dt <= end_full_dt:
        current_end_dt = current_start_dt + datetime.timedelta(days=chunk_days - 1)
        if current_end_dt > end_full_dt:
            current_end_dt = end_full_dt

        print(f"\n=== Processando chunk: {current_start_dt.date()} até {current_end_dt.date()} ===")
        start_chunk_time = datetime.datetime.now()

        # 1. Obter dados primários (pedidos, itens, clientes, funcionários)
        cur.execute("""
            SELECT P.CDPEDIDOVENDA, P.DATA, P.NOMECLIENTE, P.CDCLIENTE, P.CDFUNC, P.DESCONTO, P.VALORTOTAL
            FROM PEDIDOVENDA P JOIN EMPRESA E ON E.CNPJ = ?
            WHERE P.DATA BETWEEN ? AND ? AND P.EFETIVADO = 'S'
        """, (cnpj_loja, current_start_dt, current_end_dt))
        pedidos = cur.fetchall()

        if not pedidos:
            print("Nenhum pedido encontrado no período.")
            current_start_dt = current_end_dt + datetime.timedelta(days=1)
            continue

        pedidos_ids = [p[0] for p in pedidos]
        itens_por_pedido = defaultdict(list)
        todos_produtos_ids = set()

        for grupo in dividir_em_blocos(pedidos_ids, 1499):
            format_strings = ','.join(['?'] * len(grupo))
            cur.execute(f"""
                SELECT CDPEDIDOVENDA, CDPRODUTO, NUMORIGINAL, QUANTIDADE, VALORUNITARIOCDESC, DESCRICAO
                FROM ITENSPEDIDOVENDA WHERE CDPEDIDOVENDA IN ({format_strings})
            """, tuple(grupo))
            for cd_ped, cd_prod, num_orig, qtd, valor, desc in cur.fetchall():
                itens_por_pedido[cd_ped].append((cd_prod, num_orig, qtd, valor, desc))
                todos_produtos_ids.add(cd_prod)

        # Dados de Clientes e Fones
        cd_clientes = {p[3] for p in pedidos if p[3]}
        clientes_dict = {}
        fones_dict = {}
        if cd_clientes:
            for grupo_cli in dividir_em_blocos(list(cd_clientes), 1499):
                format_strings = ','.join(['?'] * len(grupo_cli))
                cur.execute(
                    f"SELECT CDCLIENTE, CPF_CNPJ, CEP, CIDADE, ESTADO FROM CLIENTE WHERE CDCLIENTE IN ({format_strings})", tuple(grupo_cli))
                for cd_cli, cpf, cep, cid, uf in cur.fetchall():
                    clientes_dict[cd_cli] = {
                        'cpf_cnpj': normalizar_texto(cpf),
                        'cep': normalizar_texto(cep),
                        'cidade': normalizar_texto(cid),
                        'uf': normalizar_texto(uf)}
                cur.execute(f"SELECT CDCLIENTE, FONE FROM FONE WHERE CDCLIENTE IN ({format_strings})", tuple(grupo_cli))
                for cd_cli, fone in cur.fetchall():
                    if cd_cli not in fones_dict:
                        fones_dict[cd_cli] = normalizar_texto(fone)

        # Dados de Funcionários
        cd_funcs = {p[4] for p in pedidos if p[4]}
        funcs_dict = {}
        if cd_funcs:
            format_strings = ','.join(['?'] * len(cd_funcs))
            cur.execute(f"SELECT CDFUNC, NUMCNH FROM FUNCIONARIO WHERE CDFUNC IN ({format_strings})", tuple(cd_funcs))
            for cdf, ncnh in cur.fetchall():
                ncnh_norm = normalizar_texto(ncnh).upper()
                funcs_dict[cdf] = "TELEP" if ncnh_norm == "TELEPECAS" else ncnh_norm

        # --- OTIMIZAÇÃO PRINCIPAL ---
        # 2. Pré-cache do histórico de todos os produtos do chunk
        print("Pré-carregando histórico de produtos...")
        historico_cache = pre_cache_historico_produtos(conn, list(todos_produtos_ids), current_end_dt)

        # 3. Processar histórico em memória para achar NFs de compra
        item_compra_info = {}  # (cd_ped, cd_prod) -> num_documento_compra
        num_docs_compra_necessarios = set()

        for cd_ped, itens in itens_por_pedido.items():
            for cd_prod, _, _, _, _ in itens:
                eventos_prod = historico_cache.get(cd_prod, [])

                # Encontrar a data de inserção do item no pedido
                data_insercao_item = None
                for ev in sorted(
                    [e for e in eventos_prod if e['tipo'] == 'PEDIDO' and e['numdoc'] == str(cd_ped)],
                        key=lambda x: x['data']):
                    data_insercao_item = ev['data']
                    break

                if not data_insercao_item:
                    continue

                # Encontrar a NF de compra mais recente ANTERIOR à data de inserção
                nf_compra_recente = None
                for ev in sorted(
                    [e for e in eventos_prod if e['tipo'] == 'NF COMPRA' and e['data'] <= data_insercao_item],
                    key=lambda x: x['data'],
                        reverse=True):
                    nf_compra_recente = ev['numdoc']
                    break

                if nf_compra_recente:
                    item_compra_info[(cd_ped, cd_prod)] = {'num_doc': nf_compra_recente}
                    num_docs_compra_necessarios.add(nf_compra_recente)

        # 4. Pré-cache dos dados de custo das NFs encontradas
        print(f"Pré-carregando dados de custo de {len(num_docs_compra_necessarios)} notas fiscais...")
        dados_compra_cache = pre_cache_dados_compra(conn, num_docs_compra_necessarios)

        # 5. Enriquecer 'item_compra_info' com os dados de custo
        for key, value in item_compra_info.items():
            num_doc = value['num_doc']
            cd_prod = key[1]  # cd_prod
            dados_custo = dados_compra_cache.get((num_doc, cd_prod))
            if dados_custo:
                item_compra_info[key].update(dados_custo)

        # 6. Processamento final em paralelo (sem I/O de banco)
        print("Processando pedidos em memória...")
        linhas_csv = []
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            # Submete tarefas para processamento
            futures = [executor.submit(processar_pedido_otimizado, pedido, itens_por_pedido.get(
                pedido[0], []), clientes_dict, fones_dict, funcs_dict, cnpj, item_compra_info) for pedido in pedidos]
            for fut in as_completed(futures):
                linhas_csv.extend(fut.result())

        # 7. Escrever resultados no CSV
        with open(csv_path, "a", encoding="utf-8", newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter=';', quoting=csv.QUOTE_NONE, escapechar='\\')
            writer.writerows(linhas_csv)

        end_chunk_time = datetime.datetime.now()
        print(f"Chunk finalizado em {end_chunk_time - start_chunk_time}. {len(linhas_csv)} linhas geradas.")

        # Avançar para o próximo chunk
        current_start_dt = current_end_dt + datetime.timedelta(days=1)

    conn.close()
    # enviar_arquivo_sftp(csv_path)
    print("Processamento concluído.")


if __name__ == "__main__":
    main()
