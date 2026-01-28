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
        arquivo_json = "regras_renomeador.json"
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

    def extrair_dados(self, texto):
        dados = {
            "data_pgto": "",
            "nome_recebedor": "",
            "num_doc": "",
            "descricao": "PGTO",  # Default
            "data_ref": ""
        }

        # 1. Tentar extrair DATA DE PAGAMENTO
        # Padrões comuns: 05/01/2026
        match_data = re.search(r'(\d{2}/\d{2}/\d{4})', texto)
        if match_data:
            dados["data_pgto"] = self.formatar_data(match_data.group(1))

        # 2. Aplicar Regras do JSON (Prioridade na Descrição)
        texto_upper = texto.upper()
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

        # --- BANCO INTER ---
        elif "inter" in texto.lower() and ("Pix enviado" in texto or "Comprovante" in texto):

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
        if "SALARIO" in dados["descricao"].upper():
            dados["descricao"] = "SALARIO"

        if not dados["data_ref"]:
            keywords_recorrentes = self.config.get("recorrentes", [])

            # Só adiciona data se for algo reconhecidamente recorrente ou salário
            # Se for "Compra de Carne" (que não tá na lista), fica sem data.
            eh_recorrente = any(k in desc_upper for k in keywords_recorrentes)

            if eh_recorrente:
                dados["data_ref"] = self.calcular_mes_referencia(dados["data_pgto"], desc_upper)

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
                        texto_completo += page.extract_text() + "\n"

                # Verifica se é DDA (caso especial)
                nome_dda = self.processar_bb_dda(texto_completo)

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
