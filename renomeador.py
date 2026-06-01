import os
import re
import json
import pdfplumber
import tkinter as as_tk
from tkinter import filedialog, messagebox
from tkinter.scrolledtext import ScrolledText
from datetime import datetime, timedelta


class RenomeadorComprovantes:
    def __init__(self):
        self.root = as_tk.Tk()
        self.root.title("Renomeador de Comprovantes")
        self.root.geometry("600x450")

        self.config = self.carregar_configuracao()

        # Interface Gráfica
        frame = as_tk.Frame(self.root)
        frame.pack(pady=20)

        btn_selecionar = as_tk.Button(frame, text="Selecionar Arquivos PDF",
                                      command=self.executar, font=("Arial", 12), bg="#dddddd")
        btn_selecionar.pack()

        self.log_text = ScrolledText(self.root, height=20, width=70, state='disabled')
        self.log_text.pack(pady=10, padx=10)

    def carregar_configuracao(self):
        arquivo_json = "\\\\Servidor\\Users\\Pichau\\Documents\\Drive Comagro\\regras_renomeador.json"
        config_padrao = {
            "regras": [],
            "recorrentes": [],
            "termos_ignorar": [],
            "regras_data": {}
        }

        if not os.path.exists(arquivo_json):
            try:
                with open(arquivo_json, 'w', encoding='utf-8') as f:
                    json.dump(config_padrao, f, indent=4, ensure_ascii=False)
            except:
                pass
            return config_padrao

        try:
            with open(arquivo_json, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            messagebox.showerror("Erro Config", f"Erro ao ler JSON: {e}")
            return config_padrao

    def log(self, mensagem):
        self.log_text.config(state='normal')
        self.log_text.insert(as_tk.END, mensagem + "\n")
        self.log_text.see(as_tk.END)
        self.log_text.config(state='disabled')
        self.root.update()

    def selecionar_arquivos(self):
        arquivos = filedialog.askopenfilenames(
            title="Selecione os Comprovantes PDF",
            filetypes=[("Arquivos PDF", "*.pdf")]
        )
        return arquivos

    def limpar_texto(self, texto):
        """Remove caracteres ilegais para nome de arquivo"""
        return re.sub(r'[\\/*?:"<>|]', "", texto).strip()

    def formatar_data(self, data_str):
        """Converte dd/mm/yyyy para dd-mm-yy"""
        try:
            dt = datetime.strptime(data_str, "%d/%m/%Y")
            return dt.strftime("%d-%m-%y")
        except:
            return data_str.replace("/", "-")

    def calcular_mes_referencia(self, data_pagamento, tipo_pagamento):
        """
        Calcula o mês de referência baseado na regra de negócio:
        - Padrão: Mês anterior ao pagamento.
        - Adiantamento: Mesmo mês do pagamento.
        """
        try:
            dt_pgto = datetime.strptime(data_pagamento, "%d-%m-%y")

            if "ADIANTAMENTO" in tipo_pagamento.upper() or "AGUA" in tipo_pagamento.upper() or "LUZ" in tipo_pagamento.upper():
                # Adiantamento é referente ao mês atual
                return dt_pgto.strftime("%m-%Y")
            else:
                # Regra padrão: Salário/Aluguel/Contas é referente ao mês anterior
                # Subtrai dias até virar o mês anterior
                primeiro_dia_mes_atual = dt_pgto.replace(day=1)
                ultimo_dia_mes_anterior = primeiro_dia_mes_atual - timedelta(days=1)
                return ultimo_dia_mes_anterior.strftime("%m-%Y")
        except:
            return ""

    def extrair_data_referencia(self, texto):
        """Tenta encontrar algo como 12/2025 ou 12-2025 no texto"""
        match = re.search(r'\b(0[1-9]|1[0-2])[-/](20\d{2})\b', texto)
        if match:
            return f"{match.group(1)}-{match.group(2)}"
        return ""

    def processar_bb_dda(self, texto):
        """Detecta se é o arquivo de lote DDA do BB"""
        if "Agenda de boletos DDA" in texto or "Débito Direto Autorizado" in texto:
            # Tenta achar a data principal do lote (geralmente no cabeçalho)
            data_match = re.search(r'\d{2}/\d{2}/\d{4}', texto)
            if data_match:
                data_fmt = self.formatar_data(data_match.group(0))
                return f"PAGAMENTOS_BB_{data_fmt}"
        return None

    def _data_do_caminho(self, caminho_arq):
        """Tenta extrair uma data DD-MM-YY do nome do arquivo ou da pasta pai.
        Útil para DDA Itaú em formato imagem (sem texto extraível)."""
        if not caminho_arq:
            return None

        # 1) Tenta no nome do próprio arquivo (ex: PAGAMENTOS_DDA_ITAU_29-05-26.pdf)
        nome = os.path.basename(caminho_arq)
        match = re.search(r'(\d{2})[-_/](\d{2})[-_/](\d{2,4})', nome)
        if match:
            dia, mes, ano = match.groups()
            if len(ano) == 4:
                ano = ano[-2:]
            return f"{dia}-{mes}-{ano}"

        # 2) Tenta na pasta pai (ex: ".../05. MAIO/29-05-26/arquivo.pdf")
        pasta = os.path.basename(os.path.dirname(caminho_arq))
        match_p = re.match(r'^(\d{2})[-_](\d{2})[-_](\d{2,4})$', pasta)
        if match_p:
            dia, mes, ano = match_p.groups()
            if len(ano) == 4:
                ano = ano[-2:]
            return f"{dia}-{mes}-{ano}"

        return None

    def processar_itau_dda(self, texto, caminho_arq=None):
        """Detecta se é o arquivo de lote / consolidado DDA do Itaú.

        Dois sub-casos:
        a) PDF com texto extraível (relatório "Lançamentos do período" gerado pelo Itaú web).
        b) PDF imagem (Microsoft Print To PDF), sem texto. Aí caímos em heurística pelo nome do arquivo.
        """
        # Caso A: relatório de lançamentos do Itaú (tem texto extraível)
        if "Lançamentos do período" in texto and (
            "COMAGRO" in texto.upper() or "openhtmltopdf" in texto.lower()
        ):
            data_match = re.search(r'Lançamentos do período:\s*(\d{2}/\d{2}/\d{4})', texto)
            if data_match:
                data_fmt = self.formatar_data(data_match.group(1))
                return f"PAGAMENTOS_ITAU_{data_fmt}"

        # Caso B: PDF imagem (Print To PDF do Itaú). Sem texto, mas o nome ou a pasta
        # ainda permitem reconstruir a data.
        if caminho_arq and (not texto or len(texto.strip()) < 50):
            nome_orig_upper = os.path.basename(caminho_arq).upper()
            # Heurística: nome sugere DDA / Itaú (o usuário costuma nomear assim ao baixar)
            if ("ITAU" in nome_orig_upper or "ITAÚ" in nome_orig_upper
                    or "DDA" in nome_orig_upper):
                data_fmt = self._data_do_caminho(caminho_arq)
                if data_fmt:
                    return f"PAGAMENTOS_ITAU_{data_fmt}"

        return None

    def refinar_por_data(self, grupo, data_pgto):
        if not data_pgto:
            return None
        try:
            dia = int(data_pgto.split('-')[0])
        except:
            return None

        regras_data = self.config.get("regras_data", {})
        if grupo in regras_data:
            for regra in regras_data[grupo]:
                if regra["inicio"] <= dia <= regra["fim"]:
                    return regra["descricao"]
        return None

    def _remover_rodape_legal(self, texto):
        """Remove rodapés legais padrão dos comprovantes para não contaminar regras.

        Exemplo concreto: o Itaú coloca "em dias úteis, das 9h às 18h" no rodapé,
        e o termo "das" estava sendo confundido com a sigla DAS (impostos).
        """
        marcadores_corte = [
            "Em caso de dúvidas",  # Itaú
            "Em caso de duvidas",
            "SAC 0800",
            "Ouvidoria:",
        ]
        idx_min = len(texto)
        for marcador in marcadores_corte:
            idx = texto.find(marcador)
            if idx != -1 and idx < idx_min:
                idx_min = idx
        return texto[:idx_min]

    def extrair_dados(self, texto):
        dados = {
            "data_pgto": "",
            "nome_recebedor": "",
            "num_doc": "",
            "descricao": "PGTO",  # Default
            "data_ref": ""
        }

        # Texto sem o rodapé legal padrão, usado para casar regras genéricas.
        # O texto original (`texto`) continua sendo usado nos blocos de banco,
        # porque alguns campos (autenticação, CTRL, etc.) ficam depois do rodapé.
        texto_limpo = self._remover_rodape_legal(texto)

        # 1. Tentar extrair DATA DE PAGAMENTO
        # Padrões comuns: 05/01/2026
        match_data = re.search(r'(\d{2}/\d{2}/\d{4})', texto)
        if match_data:
            dados["data_pgto"] = self.formatar_data(match_data.group(1))

        # 2. Aplicar Regras do JSON (Prioridade na Descrição)
        texto_upper = texto_limpo.upper()
        for regra in self.config.get("regras", []):
            grupo = regra.get("grupo", "")
            termos = regra.get("termos", [])
            # Usa Regex com \b para garantir palavra exata (evita que "DAS" case com "TODAS")
            if any(re.search(r'\b' + re.escape(termo.upper()) + r'\b', texto_upper) for termo in termos):
                desc_refinada = self.refinar_por_data(grupo, dados["data_pgto"])
                if desc_refinada:
                    dados["descricao"] = termos[0] + "_" + desc_refinada
                elif "AGUA" in grupo or "LUZ" in grupo:
                    dados["descricao"] = termos[0] + "_" + grupo
                else:
                    dados["descricao"] = grupo
                break

        # 3. Identificar Banco e Estrutura para Recebedor/Doc

        # Indica se já temos uma descrição específica vinda do banco (Itaú etc.).
        # Quando True, evitamos que o restante do fluxo (recorrentes, "SALARIO", etc.) sobrescreva.
        descricao_especifica = False

        # --- BANCO DO BRASIL (SISBB) ---
        if "SISBB" in texto or "BANCO DO BRASIL" in texto:
            # Recebedor
            match_fav = re.search(r'(?:FAVORECIDO|BENEFICIARIO|Convenio):\s*(.*?)\n', texto)
            if match_fav:
                dados["nome_recebedor"] = match_fav.group(1).strip()

            # Descrição / Evento
            match_evento = re.search(r'(?:EVENTO|DOCUMENTO|NR\. DOCUMENTO)[:\s]\s*(.*?)\n', texto, re.IGNORECASE)
            if match_evento:
                valor = match_evento.group(1).strip()
                if re.match(r'^[\d.-]+$', valor):  # Se for só número (aceita ponto/hifen), é numero do documento
                    dados["num_doc"] = valor
                elif dados["descricao"] == "PGTO":  # Só sobrescreve se ainda for default
                    dados["descricao"] = valor

        # --- BANCO ITAÚ ---
        # Marcadores típicos: "Banco Itaú", "Itaú Empresas", "via Sispag",
        # "agente arrecadador:CNC:341 Banco Itaú S/A".
        # IMPORTANTE: deve vir ANTES do Inter, porque o Itaú menciona "Internet"
        # no rodapé ("Itaú Empresas na Internet") e isso fazia o branch do Inter
        # capturar erroneamente o comprovante do Itaú.
        elif ("Banco Itaú" in texto or "Itaú Empresas" in texto
              or "via Sispag" in texto or "CNC:341" in texto):

            # === Caso A: DARF (CSLL, IRPJ, COFINS, etc.) ===
            if "Comprovante de pagamento - DARF" in texto:
                # Data: padrão "data do pagamento:DD/MM/AAAA"
                m_data = re.search(r'data do pagamento:\s*(\d{2}/\d{2}/\d{4})', texto, re.IGNORECASE)
                if m_data:
                    dados["data_pgto"] = self.formatar_data(m_data.group(1))

                # "identificação no extrato:" carrega o tributo legível (CSLL, IRPJ, ...)
                m_id = re.search(r'identificação no extrato:\s*(.+)', texto, re.IGNORECASE)
                if m_id:
                    id_extrato = m_id.group(1).strip().split('\n')[0].strip()
                    if id_extrato:
                        dados["descricao"] = id_extrato
                        descricao_especifica = True

                # Número do documento do DARF (útil como rastreio)
                m_ndoc = re.search(r'número do documento:\s*([\d.\-]+)', texto, re.IGNORECASE)
                if m_ndoc and not dados["num_doc"]:
                    dados["num_doc"] = m_ndoc.group(1).strip()

                # Recebedor padrão para DARF é a Receita Federal
                if not dados["nome_recebedor"]:
                    dados["nome_recebedor"] = "RECEITA FEDERAL"

            # === Caso B: Boleto comum (Cartão Caixa, fornecedor, etc.) ===
            elif "Comprovante de pagamento de boleto" in texto:
                # Data: "Data de pagamento: DD/MM/AAAA"
                m_data_b = re.search(r'Data de pagamento:\s*(\d{2}/\d{2}/\d{4})', texto, re.IGNORECASE)
                if m_data_b:
                    dados["data_pgto"] = self.formatar_data(m_data_b.group(1))

                # Beneficiário: a "Razão Social" costuma vir mais limpa que "Beneficiário".
                m_razao = re.search(r'Razão Social:\s*(.+?)(?:\s+\d{2}\.\d{3}\.\d{3}/|\n)', texto)
                if m_razao:
                    dados["nome_recebedor"] = m_razao.group(1).strip()
                else:
                    m_benef = re.search(r'Beneficiário:\s*(.+?)(?:\s+CPF|\n)', texto)
                    if m_benef:
                        dados["nome_recebedor"] = m_benef.group(1).strip()

            # === Caso C: Concessionárias (VIVO, COELBA via Itaú, etc.) ===
            elif "Comprovante de Pagamento de concessionárias" in texto:
                # Data: "Operação efetuada em DD/MM/AAAA"
                m_data_c = re.search(r'Operação efetuada em (\d{2}/\d{2}/\d{4})', texto)
                if m_data_c:
                    dados["data_pgto"] = self.formatar_data(m_data_c.group(1))

                # As "Informações fornecidas pelo pagador" trazem o rótulo legível
                # (ex: "VIVO INTERNET LOJA", "VIVO MONITORA OFICINA"). Por causa do
                # layout em colunas, o pdfplumber às vezes intercala "pagador:"
                # entre o rótulo "Informações fornecidas pelo" e o valor.
                info_pagador = ""
                m_info = re.search(
                    r'Informações fornecidas pelo\s*\n?\s*(.+?)\s*\n?\s*pagador:',
                    texto, re.IGNORECASE | re.DOTALL
                )
                if m_info:
                    info_pagador = re.sub(r'\s+', ' ', m_info.group(1)).strip()
                else:
                    # Fallback: layout em colunas. Ex.:
                    #   "Informações fornecidas pelo"
                    #   "VIVO INTERNET LOJA"
                    #   "pagador:"
                    idx = texto.find("Informações fornecidas pelo")
                    if idx != -1:
                        bloco = texto[idx:].split('\n')
                        for linha in bloco[1:6]:
                            lin = linha.strip()
                            if not lin or lin.lower().startswith("pagador"):
                                continue
                            info_pagador = lin
                            break

                if info_pagador:
                    dados["descricao"] = info_pagador
                    descricao_especifica = True

                # Concessionária (linha "0041 - VIVO-BA" / "0084 - TELEFONICA EMPRESAS")
                # serve como nome do recebedor caso a info do pagador esteja vazia.
                if not dados["nome_recebedor"]:
                    m_conc = re.search(r'^\s*\d{4}\s*-\s*(.+)$', texto, re.MULTILINE)
                    if m_conc and not info_pagador:
                        dados["nome_recebedor"] = m_conc.group(1).strip()

        # --- BANCO INTER ---
        # Detecção específica: o Inter usa "Banco Inter S.A." no comprovante e/ou
        # "Pix enviado". Antes a regra era apenas "inter" no lower(), o que dava
        # falso positivo no Itaú (que tem "Internet" no rodapé legal).
        elif (("Banco Inter" in texto or "banco inter" in texto.lower()
               or "Pix enviado" in texto)
              and ("Pix enviado" in texto or "Comprovante" in texto)):

            # 1. DESCRIÇÃO: Pega o que vier depois de "Descrição" (sem aspas)
            # Aceita: "Descrição: Blabla" ou "Descrição \n Blabla"
            match_desc = re.search(r'Descrição\s*[:\n]?\s*(.+)', texto, re.IGNORECASE)
            if match_desc:
                raw_desc = match_desc.group(1).replace('"', '').strip()
                # Se pegou linha errada ou vazia, ignora
                if len(raw_desc) > 2 and dados["descricao"] == "PGTO":
                    dados["descricao"] = raw_desc
                    # Tenta extrair data da descrição achada
                    ref = self.extrair_data_referencia(raw_desc)
                    if ref:
                        dados["data_ref"] = ref
                        # Remove a data (ex: 12/2025) da descrição para não duplicar no nome final
                        match_ref_str = re.search(r'\b(0[1-9]|1[0-2])[-/](20\d{2})\b', raw_desc)
                        if match_ref_str:
                            dados["descricao"] = dados["descricao"].replace(match_ref_str.group(0), "")

            # 2. QUEM RECEBEU: Lógica de exclusão linha a linha
            idx_recebedor = texto.find("Quem recebeu")
            if idx_recebedor != -1:
                # Pega um pedaço do texto após "Quem recebeu" e divide em linhas
                bloco = texto[idx_recebedor:].split('\n')

                # Vamos varrer as próximas 15 linhas procurando o nome
                for linha in bloco[1:15]:
                    lin = linha.strip().replace('"', '')  # Limpa aspas se houver

                    # Pula linhas vazias ou cabeçalhos conhecidos do Inter
                    if not lin or lin.upper() in ["NOME", "QUEM RECEBEU", "DADOS DO RECEBEDOR"]:
                        continue

                    # Pula linhas que são claramente metadados
                    if any(x in lin.upper() for x in
                           ["CPF", "CNPJ", "INSTITUIÇÃO", "AGÊNCIA", "CONTA", "CHAVE", "TIPO"]):
                        continue

                    # Se a linha começar com "Nome ", pegamos o resto
                    if lin.startswith("Nome "):
                        dados["nome_recebedor"] = lin[5:].strip()
                        break

                    # Se chegou aqui, é muito provável que seja o nome (ex: "Notliv Patrimonial Ltda")
                    dados["nome_recebedor"] = lin
                    break

        desc_upper = dados["descricao"].upper()

        dados["descricao"] = dados["descricao"].replace("PGTO", "")
        dados["descricao"] = dados["descricao"].replace("-", "")
        dados["descricao"] = dados["descricao"].replace("/", "-")

        # Remover termos a ignorar
        for termo in self.config.get("termos_ignorar", []):
            dados["descricao"] = dados["descricao"].replace(termo, "").strip()

        # Se a descrição tiver "SALARIO", simplifica
        # (pula caso a descrição já tenha sido fixada por um banco específico,
        # para não estragar coisas como "VIVO INTERNET LOJA" se um dia algum
        # comprovante do Itaú vier com a palavra "SALARIO" embutida)
        if "SALARIO" in dados["descricao"].upper() and not descricao_especifica:
            dados["descricao"] = "SALARIO"

        if not dados["data_ref"]:
            keywords_recorrentes = self.config.get("recorrentes", [])

            # Para a checagem de recorrência, consideramos descrição + nome do recebedor.
            # Motivo: alguns boletos (ex: Cartão Caixa via Itaú) não têm descrição própria,
            # mas o termo recorrente aparece no recebedor (ex: "CARTOES CAIXA VISA PF").
            texto_busca = (dados["descricao"] + " " + dados["nome_recebedor"]).upper()

            # Só adiciona data se for algo reconhecidamente recorrente ou salário
            # Se for "Compra de Carne" (que não tá na lista), fica sem data.
            eh_recorrente = any(k in texto_busca for k in keywords_recorrentes)

            if eh_recorrente:
                dados["data_ref"] = self.calcular_mes_referencia(dados["data_pgto"], texto_busca)

        # Se não achou recebedor, tenta pegar de linhas genéricas de boleto
        if not dados["nome_recebedor"]:
            match_benef = re.search(r'BENEFICIARIO:\s*(.*?)\n', texto)
            if match_benef:
                dados["nome_recebedor"] = match_benef.group(1)

        return dados

    def gerar_novo_nome(self, dados, original_filename):
        # Se data não foi achada, usa a data de hoje como fallback (ruim, mas evita crash)
        data = dados["data_pgto"] if dados["data_pgto"] else datetime.now().strftime("%d-%m-%y")

        # Monta partes
        partes = [data, "PGTO"]

        if dados["nome_recebedor"]:
            # Pega só o primeiro e último nome ou as 3 primeiras palavras para não ficar gigante
            nome = self.limpar_texto(dados["nome_recebedor"]).replace(" ", "_").upper()
            partes.append(nome)

        if dados["descricao"] and dados["descricao"] != "PGTO":
            desc = self.limpar_texto(dados["descricao"]).replace(" ", "_").upper()
            partes.append(desc)

        if dados["data_ref"]:
            partes.append(dados["data_ref"].replace("/", "-"))

        if dados["num_doc"]:
            partes.append(dados["num_doc"])

        # Junta tudo
        novo_nome = "_".join(partes) + ".pdf"

        # Remove duplicidade de underscores
        novo_nome = re.sub(r'_{2,}', '_', novo_nome)

        return novo_nome

    def executar(self):
        arquivos = self.selecionar_arquivos()
        if not arquivos:
            return

        sucessos = 0
        erros = 0
        self.log("Iniciando processamento...")

        for caminho_arq in arquivos:
            try:
                with pdfplumber.open(caminho_arq) as pdf:
                    # Pega texto da primeira página (suficiente para a maioria)
                    # Para DDA, pegamos tudo para garantir
                    texto_completo = ""
                    for page in pdf.pages:
                        # Em PDFs imagem (Print To PDF), extract_text pode retornar None
                        page_text = page.extract_text() or ""
                        texto_completo += page_text + "\n"

                # Verifica se é DDA / consolidado (caso especial)
                # Tenta primeiro o BB, depois o Itaú (inclui caso de PDF imagem).
                nome_dda = (
                    self.processar_bb_dda(texto_completo)
                    or self.processar_itau_dda(texto_completo, caminho_arq)
                )

                pasta = os.path.dirname(caminho_arq)

                if nome_dda:
                    novo_nome = nome_dda + ".pdf"
                else:
                    dados = self.extrair_dados(texto_completo)
                    novo_nome = self.gerar_novo_nome(dados, os.path.basename(caminho_arq))

                novo_caminho = os.path.join(pasta, novo_nome)

                # Renomear
                os.rename(caminho_arq, novo_caminho)
                self.log(f"OK: {os.path.basename(caminho_arq)} -> {novo_nome}")
                sucessos += 1

            except Exception as e:
                erros += 1
                self.log(f"ERRO em {os.path.basename(caminho_arq)}: {str(e)}")

        # Relatório final
        msg = f"Processados: {sucessos}\nErros: {erros}"
        messagebox.showinfo("Concluído", msg)
        # self.root.destroy() # Manter janela aberta


if __name__ == "__main__":
    app = RenomeadorComprovantes()
    app.root.mainloop()
