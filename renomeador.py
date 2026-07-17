import os
import re
import json
import shutil
import subprocess
import tempfile
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
        self._ocr_bin = None  # cache do caminho do helper Vision (macOS)

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
            except Exception:
                pass
            return config_padrao

        try:
            with open(arquivo_json, 'r', encoding='utf-8') as f:
                config = json.load(f)
            config["_caminho_carregado"] = arquivo_json
            return config
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
        except Exception:
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
        except Exception:
            return ""

    def extrair_data_referencia(self, texto):
        """Tenta encontrar algo como 12/2025 ou 12-2025 no texto.

        Evita falso positivo dentro de data completa (ex: 17/07/2026 → não é 07/2026).
        """
        match = re.search(r'(?<!\d{2}/)(?<!\d)(0[1-9]|1[0-2])[-/](20\d{2})\b', texto)
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

    def _identificacao_boleto_itau(self, texto):
        """Extrai o campo 'Identificação no meu comprovante' do boleto Itaú.

        Retorna (valor, preenchida):
        - preenchida=True  → boleto avulso (usuário digitou algo legível, ex: 'Cartao Caixa')
        - preenchida=False → DDA (campo vazio; o que sobra é a linha digitável/código de barras)
        """
        m = re.search(r'Identifica[cç][aã]o no meu comprovante:\s*(.*)', texto, re.IGNORECASE)
        if not m:
            return "", False
        valor = m.group(1).strip().split('\n')[0].strip()
        # Linha digitável: só dígitos/espaços/pontos e bem longa
        so_codigo = bool(re.fullmatch(r'[\d\s.]+', valor)) and len(re.sub(r'\s', '', valor)) >= 20
        if not valor or so_codigo:
            return "", False
        return valor, True

    def processar_itau_dda(self, texto, caminho_arq=None, num_paginas=1):
        """Detecta se é o arquivo de lote / consolidado DDA do Itaú.

        Sub-casos:
        a) Relatório "Lançamentos do período" (texto extraível do Itaú web).
        b) Lote de comprovantes de boleto Sispag (mesmo layout do avulso):
           - DDA: sempre >1 página e identificação vazia
           - Avulso: 1 página com identificação preenchida pelo usuário
        c) PDF imagem (Print To PDF), sem texto — heurística pelo nome/pasta.
        """
        # Caso A: relatório de lançamentos do Itaú (tem texto extraível)
        if "Lançamentos do período" in texto and (
            "COMAGRO" in texto.upper() or "openhtmltopdf" in texto.lower()
        ):
            data_match = re.search(r'Lançamentos do período:\s*(\d{2}/\d{2}/\d{4})', texto)
            if data_match:
                data_fmt = self.formatar_data(data_match.group(1))
                return f"PAGAMENTOS_DDA_ITAU_{data_fmt}"

        # Caso B: comprovantes de boleto via Sispag (DDA multipágina vs avulso 1 página)
        eh_boleto_sispag = (
            "Comprovante de pagamento de boleto" in texto
            and ("via Sispag" in texto or "CNC:341" in texto)
        )
        if eh_boleto_sispag:
            # DDA consolidado: sempre vem com mais de uma página
            if num_paginas and num_paginas > 1:
                data_match = re.search(r'Data de pagamento:\s*(\d{2}/\d{2}/\d{4})', texto, re.IGNORECASE)
                if data_match:
                    data_fmt = self.formatar_data(data_match.group(1))
                else:
                    data_fmt = self._data_do_caminho(caminho_arq)
                if data_fmt:
                    return f"PAGAMENTOS_DDA_ITAU_{data_fmt}"

            # 1 página: se a identificação estiver vazia, ainda trata como DDA (lote de 1)
            _, ident_ok = self._identificacao_boleto_itau(texto)
            if not ident_ok:
                data_match = re.search(r'Data de pagamento:\s*(\d{2}/\d{2}/\d{4})', texto, re.IGNORECASE)
                if data_match:
                    data_fmt = self.formatar_data(data_match.group(1))
                else:
                    data_fmt = self._data_do_caminho(caminho_arq)
                if data_fmt:
                    return f"PAGAMENTOS_DDA_ITAU_{data_fmt}"
            # Identificação preenchida + 1 página = boleto avulso → não é DDA
            return None

        # Caso C: PDF imagem (Print To PDF do Itaú). Sem texto, mas o nome ou a pasta
        # ainda permitem reconstruir a data.
        if caminho_arq and (not texto or len(texto.strip()) < 50):
            nome_orig_upper = os.path.basename(caminho_arq).upper()
            # Heurística: nome sugere DDA / Itaú (o usuário costuma nomear assim ao baixar)
            if ("ITAU" in nome_orig_upper or "ITAÚ" in nome_orig_upper
                    or "DDA" in nome_orig_upper):
                data_fmt = self._data_do_caminho(caminho_arq)
                if data_fmt:
                    return f"PAGAMENTOS_DDA_ITAU_{data_fmt}"

        return None

    def refinar_por_data(self, grupo, data_pgto):
        if not data_pgto:
            return None
        try:
            dia = int(data_pgto.split('-')[0])
        except Exception:
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
            "Fale com a gente",  # Inter
        ]
        idx_min = len(texto)
        for marcador in marcadores_corte:
            idx = texto.find(marcador)
            if idx != -1 and idx < idx_min:
                idx_min = idx
        return texto[:idx_min]

    def _texto_sem_acento(self, texto):
        """Normaliza acentos para comparações (ITAÚ → ITAU)."""
        mapa = str.maketrans({
            "Á": "A", "À": "A", "Ã": "A", "Â": "A",
            "É": "E", "Ê": "E",
            "Í": "I",
            "Ó": "O", "Õ": "O", "Ô": "O",
            "Ú": "U", "Ü": "U",
            "Ç": "C",
        })
        return texto.upper().translate(mapa)

    def _termo_presente(self, termo, texto_upper):
        """Verifica se o termo aparece no texto sem falso positivo tipo DAS⊂TODAS.

        Usa fronteira alfanumérica (não \\b), para casar SECULOS dentro de
        MENSALIDADE_SISTEMA_SECULOS (underscore não quebra \\b do Python).
        """
        t = termo.upper()
        if not t:
            return False
        # (?<![A-Z0-9]) termo (?![A-Z0-9]) — underscore/_hífen contam como separador
        padrao = r'(?<![A-Z0-9])' + re.escape(t) + r'(?![A-Z0-9])'
        return re.search(padrao, texto_upper) is not None

    def _aplicar_regras_json(self, texto_limpo, dados):
        """Aplica as regras do JSON na descrição. Retorna True se casou alguma."""
        texto_upper = texto_limpo.upper()
        texto_norm = self._texto_sem_acento(texto_limpo)

        for regra in self.config.get("regras", []):
            grupo = regra.get("grupo", "")
            termos = regra.get("termos", [])
            termo_casado = None
            for termo in termos:
                if self._termo_presente(termo, texto_upper):
                    termo_casado = termo.upper()
                    break
            if not termo_casado:
                continue

            # Desambiguação INTERNO: "INTERNO" sozinho precisa do banco de destino
            if grupo.startswith("INTERNO_") and termo_casado == "INTERNO":
                if "ITAU" in grupo.upper():
                    if "ITAU" not in texto_norm:
                        continue
                elif "BNB" in grupo.upper():
                    if "BNB" not in texto_norm:
                        continue

            desc_refinada = self.refinar_por_data(grupo, dados["data_pgto"])
            if desc_refinada:
                dados["descricao"] = termos[0] + "_" + desc_refinada
            elif "AGUA" in grupo or "LUZ" in grupo:
                dados["descricao"] = termos[0] + "_" + grupo
            else:
                dados["descricao"] = grupo

            # Preserva mês explícito no comprovante (ex: MENSALIDADE_SISTEMA_SECULOS 07/2026)
            if not dados.get("data_ref"):
                dados["data_ref"] = self.extrair_data_referencia(texto_limpo)
            return True
        return False

    # --- OCR (Inter imprime valores como curvas; pdfplumber só vê os rótulos) ---

    # Script PowerShell embutido: assim o OCR Windows funciona mesmo se o .ps1
    # nao estiver na mesma pasta do renomeador.py (caso comum no Servidor).
    _OCR_WINDOWS_PS1 = r'''
param([Parameter(Mandatory=$true)][string]$Path)
$ErrorActionPreference = 'Stop'
if (-not (Test-Path -LiteralPath $Path)) { Write-Error "Arquivo nao encontrado: $Path"; exit 2 }
$Path = (Resolve-Path -LiteralPath $Path).Path
Add-Type -AssemblyName System.Runtime.WindowsRuntime | Out-Null
$null = [Windows.Storage.StorageFile,Windows.Storage,ContentType=WindowsRuntime]
$null = [Windows.Media.Ocr.OcrEngine,Windows.Foundation,ContentType=WindowsRuntime]
$null = [Windows.Graphics.Imaging.BitmapDecoder,Windows.Foundation,ContentType=WindowsRuntime]
$null = [Windows.Graphics.Imaging.SoftwareBitmap,Windows.Foundation,ContentType=WindowsRuntime]
$null = [Windows.Storage.Streams.RandomAccessStream,Windows.Storage.Streams,ContentType=WindowsRuntime]
$null = [Windows.Globalization.Language,Windows.Foundation,ContentType=WindowsRuntime]
$getAwaiter = [WindowsRuntimeSystemExtensions].GetMember('GetAwaiter').Where({
    $PSItem.GetParameters()[0].ParameterType.Name -eq 'IAsyncOperation`1'
}, 'First')[0]
function Await-WinRT($AsyncTask, [Type]$ResultType) {
    $getAwaiter.MakeGenericMethod($ResultType).Invoke($null, @($AsyncTask)).GetResult()
}
$engine = $null
$tentativas = @('pt-BR','pt-PT','en-US','en-GB')
foreach ($tag in $tentativas) {
    try {
        $lang = [Windows.Globalization.Language]::new($tag)
        $engine = [Windows.Media.Ocr.OcrEngine]::TryCreateFromLanguage($lang)
        if ($engine) { break }
    } catch {}
}
if (-not $engine) {
    try { $engine = [Windows.Media.Ocr.OcrEngine]::TryCreateFromUserProfileLanguages() } catch {}
}
if (-not $engine) {
    $disponiveis = @()
    try {
        foreach ($l in [Windows.Media.Ocr.OcrEngine]::AvailableRecognizerLanguages) {
            $disponiveis += $l.LanguageTag
        }
    } catch {}
    $lista = if ($disponiveis.Count) { $disponiveis -join ', ' } else { '(nenhum)' }
    [Console]::Error.WriteLine("OCR_ENGINE_MISSING langs=$lista")
    Write-Error "Nenhum motor OCR do Windows disponivel. Idiomas OCR instalados: $lista. Instale 'OCR do idioma' em Configuracoes > Hora e idioma > Idioma, ou: pip install rapidocr-onnxruntime"
    exit 3
}
$file = Await-WinRT ([Windows.Storage.StorageFile]::GetFileFromPathAsync($Path)) ([Windows.Storage.StorageFile])
$stream = Await-WinRT ($file.OpenAsync([Windows.Storage.FileAccessMode]::Read)) ([Windows.Storage.Streams.IRandomAccessStream])
$decoder = Await-WinRT ([Windows.Graphics.Imaging.BitmapDecoder]::CreateAsync($stream)) ([Windows.Graphics.Imaging.BitmapDecoder])
$bitmap = Await-WinRT ($decoder.GetSoftwareBitmapAsync()) ([Windows.Graphics.Imaging.SoftwareBitmap])
$result = Await-WinRT ($engine.RecognizeAsync($bitmap)) ([Windows.Media.Ocr.OcrResult])
foreach ($line in $result.Lines) { Write-Output $line.Text }
'''

    def _texto_precisa_ocr(self, texto):
        """Heurística: comprovante Inter sem valores extraíveis (só labels)."""
        if "Internet Banking Inter" in texto or "contadigital.inter.co" in texto:
            # Se não tem "Pix" nem "R$", os valores estão como outline/curva
            if "Pix" not in texto and "R$" not in texto:
                return True
        return False

    def _garantir_ocr_helper_mac(self):
        """Compila (se preciso) e devolve o binário ocr_vision_helper no macOS."""
        if self._ocr_bin and os.path.exists(self._ocr_bin):
            return self._ocr_bin

        script_dir = os.path.dirname(os.path.abspath(__file__))
        bin_path = os.path.join(script_dir, "ocr_vision_helper")
        src_path = os.path.join(script_dir, "ocr_vision.swift")

        if os.path.exists(bin_path) and os.access(bin_path, os.X_OK):
            self._ocr_bin = bin_path
            return bin_path

        swiftc = shutil.which("swiftc")
        if not swiftc or not os.path.exists(src_path):
            return None
        try:
            subprocess.run(
                [swiftc, src_path, "-o", bin_path],
                check=True, capture_output=True, timeout=120
            )
            self._ocr_bin = bin_path
            return bin_path
        except Exception:
            return None

    def _powershell_51(self):
        """Sempre usa Windows PowerShell 5.1 (WinRT OCR quebra no PowerShell 7)."""
        candidatos = [
            os.path.expandvars(r"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"),
            r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe",
        ]
        for c in candidatos:
            if c and os.path.exists(c):
                return c
        # Último recurso: o que estiver no PATH (pode ser PS7 e falhar)
        return shutil.which("powershell") or shutil.which("powershell.exe")

    def _ocr_via_windows(self, caminho_png):
        """OCR nativo do Windows 10/11 (Windows.Media.Ocr via PowerShell 5.1)."""
        powershell = self._powershell_51()
        if not powershell:
            self.log("  OCR Windows: powershell.exe nao encontrado")
            return ""

        # Garante caminho absoluto (WinRT StorageFile exige)
        caminho_png = os.path.abspath(caminho_png)

        # Prefere o .ps1 ao lado do script; senão grava o embutido num temp
        script_dir = os.path.dirname(os.path.abspath(__file__))
        ps1 = os.path.join(script_dir, "ocr_windows.ps1")
        ps1_temp = None
        if not os.path.exists(ps1):
            try:
                fd, ps1_temp = tempfile.mkstemp(suffix=".ps1")
                os.close(fd)
                with open(ps1_temp, "w", encoding="utf-8") as f:
                    f.write(self._OCR_WINDOWS_PS1)
                ps1 = ps1_temp
                self.log("  OCR Windows: usando script embutido (ocr_windows.ps1 ausente)")
            except Exception as e:
                self.log(f"  OCR Windows: nao consegui gravar script temp ({e})")
                return ""

        try:
            # creationflags só existe no Windows
            kwargs = {"capture_output": True, "text": True, "timeout": 90}
            if os.name == "nt":
                kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW

            r = subprocess.run(
                [
                    powershell,
                    "-NoProfile",
                    "-ExecutionPolicy", "Bypass",
                    "-File", ps1,
                    "-Path", caminho_png,
                ],
                **kwargs,
            )
            out = (r.stdout or "").strip()
            err = (r.stderr or "").strip()
            if not out:
                if "OCR_ENGINE_MISSING" in err or "Nenhum motor OCR" in err:
                    self.log(
                        "  OCR Windows: pacote de idioma OCR nao instalado no Windows. "
                        "Instale pt-BR/en-US em Configuracoes > Idioma, "
                        "OU rode: pip install rapidocr-onnxruntime"
                    )
                elif err:
                    self.log(f"  OCR Windows stderr: {err[:400]}")
                elif r.returncode:
                    self.log(f"  OCR Windows exit={r.returncode} (sem texto)")
            return out
        except Exception as e:
            self.log(f"  OCR Windows falhou: {e}")
            return ""
        finally:
            if ps1_temp:
                try:
                    os.unlink(ps1_temp)
                except Exception:
                    pass

    def _ocr_via_rapidocr(self, caminho_png):
        """Fallback Python puro (pip install rapidocr-onnxruntime) — nao depende do Windows."""
        try:
            from rapidocr_onnxruntime import RapidOCR
        except ImportError:
            return ""
        try:
            # Cache do engine na instancia (modelo demora a carregar na 1ª vez)
            if not getattr(self, "_rapidocr_engine", None):
                self.log("  OCR RapidOCR: carregando modelo (1a vez pode demorar)...")
                self._rapidocr_engine = RapidOCR()
            result, _ = self._rapidocr_engine(caminho_png)
            if not result:
                return ""
            # Cada item: [box, texto, confianca]
            return "\n".join(item[1] for item in result if item and len(item) > 1).strip()
        except Exception as e:
            self.log(f"  OCR RapidOCR falhou: {e}")
            return ""

    def _ocr_via_tesseract(self, caminho_png):
        """Fallback se o Tesseract estiver instalado no PATH."""
        if not shutil.which("tesseract"):
            return ""
        try:
            r = subprocess.run(
                ["tesseract", caminho_png, "stdout", "-l", "por+eng"],
                capture_output=True, text=True, timeout=60
            )
            return (r.stdout or "").strip()
        except Exception as e:
            self.log(f"  OCR tesseract falhou: {e}")
            return ""

    def _ocr_via_mac_vision(self, caminho_png):
        """OCR via Vision (macOS), se o helper estiver disponível."""
        helper = self._garantir_ocr_helper_mac()
        if not helper:
            return ""
        try:
            r = subprocess.run(
                [helper, caminho_png],
                capture_output=True, text=True, timeout=60
            )
            return (r.stdout or "").strip()
        except Exception as e:
            self.log(f"  OCR Mac Vision falhou: {e}")
            return ""

    def _ocr_primeira_pagina(self, caminho_arq):
        """Renderiza a 1ª página e roda OCR (Windows → RapidOCR → Mac → tesseract)."""
        tmp_path = None
        try:
            with pdfplumber.open(caminho_arq) as pdf:
                if not pdf.pages:
                    return ""
                try:
                    # Resolução um pouco maior ajuda o OCR nos laranjas do Inter
                    im = pdf.pages[0].to_image(resolution=180)
                except Exception as e_img:
                    self.log(
                        f"  OCR: nao consegui renderizar a pagina ({e_img}). "
                        f"Confirme: pip install pypdfium2"
                    )
                    return ""

                # Caminho simples em TEMP — WinRT às vezes falha com nomes estranhos
                tmp_dir = tempfile.gettempdir()
                tmp_path = os.path.join(tmp_dir, "renomeador_ocr_page.png")
                im.save(tmp_path)

            tentativas = [
                ("Windows", self._ocr_via_windows),
                ("RapidOCR", self._ocr_via_rapidocr),
                ("Mac Vision", self._ocr_via_mac_vision),
                ("tesseract", self._ocr_via_tesseract),
            ]
            for nome, fn in tentativas:
                if nome == "Windows" and os.name != "nt":
                    continue
                if nome == "Mac Vision" and os.name == "nt":
                    continue
                texto = fn(tmp_path)
                if texto and len(texto) > 40:
                    self.log(f"  OCR ok via {nome} ({len(texto)} chars)")
                    return texto

            # Nenhum motor funcionou — mensagem acionável
            tem_rapid = True
            try:
                import rapidocr_onnxruntime  # noqa: F401
            except ImportError:
                tem_rapid = False
            if not tem_rapid:
                self.log(
                    "  OCR: nenhum motor disponivel. No Windows rode UM destes:\n"
                    "    pip install rapidocr-onnxruntime\n"
                    "  ou instale o pacote 'OCR' do idioma (pt-BR/en-US) em\n"
                    "    Configuracoes > Hora e idioma > Idioma e regiao"
                )
            return ""
        except Exception as e:
            self.log(f"  OCR falhou: {e}")
            return ""
        finally:
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass

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
        self._aplicar_regras_json(texto_limpo, dados)

        # 3. Identificar Banco e Estrutura para Recebedor/Doc

        # Indica se já temos uma descrição específica vinda do banco (Itaú etc.).
        # Quando True, evitamos que o restante do fluxo (recorrentes, "SALARIO", etc.) sobrescreva.
        descricao_especifica = dados["descricao"] != "PGTO"

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

                # Identificação digitada no avulso (ex: "Cartao Caixa") — prioridade na descrição
                ident, ident_ok = self._identificacao_boleto_itau(texto)
                if ident_ok and dados["descricao"] == "PGTO":
                    dados["descricao"] = ident
                    descricao_especifica = True

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
        # Detecção específica: o Inter usa "Banco Inter S.A." / "Internet Banking Inter"
        # / "Pix enviado" / "Pix recebido". Antes a regra era apenas "inter" no lower(),
        # o que dava falso positivo no Itaú (que tem "Internet" no rodapé legal).
        elif (("Internet Banking Inter" in texto or "contadigital.inter.co" in texto
               or "Banco Inter" in texto or "banco inter" in texto.lower()
               or "Pix enviado" in texto or "Pix recebido" in texto)
              and ("Pix" in texto or "Comprovante" in texto or "Internet Banking Inter" in texto)):

            self._extrair_inter(texto, dados)
            if dados["descricao"] != "PGTO":
                descricao_especifica = True

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

    def _extrair_inter(self, texto, dados):
        """Extrai data / descrição / recebedor de comprovantes do Inter.

        Funciona tanto com texto nativo quanto com OCR (Vision), onde os valores
        costumam aparecer depois de todos os rótulos.
        """
        # Data da transação: prefere a que vem logo após o rótulo; senão a 1ª do corpo
        # (no OCR a data de impressão do rodapé costuma ser a última).
        m_data_tx = re.search(
            r'Data da transa[cç][aã]o\s*.*?(\d{2}/\d{2}/\d{4})',
            texto, re.IGNORECASE | re.DOTALL
        )
        if m_data_tx:
            dados["data_pgto"] = self.formatar_data(m_data_tx.group(1))
        else:
            datas = re.findall(r'(\d{2}/\d{2}/\d{4})', texto)
            if datas:
                # Se houver 2+ datas, a de impressão costuma ser a última
                dados["data_pgto"] = self.formatar_data(datas[0] if len(datas) == 1 else datas[0])
                # Heurística OCR: primeira data após URL do Inter é a da transação
                idx_url = texto.find("contadigital.inter.co")
                if idx_url != -1:
                    datas_apos = re.findall(r'(\d{2}/\d{2}/\d{4})', texto[idx_url:])
                    if datas_apos:
                        dados["data_pgto"] = self.formatar_data(datas_apos[0])

        # Descrição explícita "PGTO - ..."
        if dados["descricao"] == "PGTO":
            m_pgto = re.search(r'PGTO\s*[-–]\s*(.+)', texto, re.IGNORECASE)
            if m_pgto:
                # Pega só a linha (OCR às vezes cola CPF na mesma linha do nome, não da desc)
                desc = m_pgto.group(0).strip().split('\n')[0].strip()
                dados["descricao"] = desc
                ref = self.extrair_data_referencia(desc)
                if ref:
                    dados["data_ref"] = ref
                    match_ref_str = re.search(r'\b(0[1-9]|1[0-2])[-/](20\d{2})\b', desc)
                    if match_ref_str:
                        dados["descricao"] = dados["descricao"].replace(match_ref_str.group(0), "").strip()
            else:
                # Layout clássico: "Descrição" seguido do valor na mesma/próxima linha
                match_desc = re.search(r'Descri[cç][aã]o\s*[:\n]?\s*(.+)', texto, re.IGNORECASE)
                if match_desc:
                    raw_desc = match_desc.group(1).replace('"', '').strip().split('\n')[0].strip()
                    # Evita pegar o próximo rótulo ("Quem pagou", "Nome", ...)
                    rotulos = {"QUEM PAGOU", "QUEM RECEBEU", "NOME", "CPF/CNPJ", "INSTITUIÇÃO", "INSTITUICAO"}
                    if len(raw_desc) > 2 and raw_desc.upper() not in rotulos:
                        dados["descricao"] = raw_desc

        # Título do comprovante quando não há descrição (ex: Pix recebido devolvido)
        if dados["descricao"] == "PGTO":
            for titulo in ("Pix recebido devolvido", "Pix enviado", "Pix recebido"):
                if titulo in texto:
                    dados["descricao"] = titulo
                    break

        # Recebedor — no OCR os valores vêm depois de todos os labels; pulamos lixo óbvio
        if not dados["nome_recebedor"]:
            idx_recebedor = texto.find("Quem recebeu")
            if idx_recebedor == -1:
                return
            # No OCR, o bloco de valores começa após a URL do extrato
            idx_vals = texto.find("contadigital.inter.co", idx_recebedor)
            inicio = idx_vals if idx_vals != -1 else idx_recebedor
            bloco = texto[inicio:].split('\n')

            # Ordem típica dos valores no OCR (Pix enviado com descrição):
            # data, hora, id, [descrição], pagador..., recebedor(nome)...
            # Sem descrição: data, hora, id, pagador..., recebedor...
            candidatos = []
            for linha in bloco:
                lin = linha.strip().replace('"', '')
                if not lin:
                    continue
                lin_up = lin.upper()
                if lin_up in ["NOME", "QUEM RECEBEU", "DADOS DO RECEBEDOR", "CPF/CNPJ",
                              "INSTITUIÇÃO", "INSTITUICAO", "AGÊNCIA", "AGENCIA", "CONTA",
                              "CHAVE", "TIPO", "CACC", "SVGS", "SOBRE A TRANSAÇÃO",
                              "QUEM PAGOU", "DESCRIÇÃO", "DESCRICAO"]:
                    continue
                if any(x in lin_up for x in ["HTTPS://", "FALE COM", "CAPITAIS", "OUVIDORIA",
                                              "DEFICIÊNCIA", "DEFICIENCIA", "DEMAIS LOCAL"]):
                    continue
                if re.match(r'^\d{2}/\d{2}/\d{4}', lin):
                    continue
                if re.match(r'^\d{1,2}h\d{2}', lin, re.IGNORECASE):
                    continue
                if re.match(r'^[ED]\d{10,}', lin):  # ID da transação Inter
                    continue
                if lin_up.startswith("PGTO"):
                    continue
                if re.fullmatch(r'R\$\s*[\d.,]+', lin_up):
                    continue
                if re.fullmatch(r'[\d.\-*/]+', lin):
                    continue
                if lin.startswith("Nome "):
                    candidatos.append(lin[5:].strip())
                    continue
                candidatos.append(lin)

            # Pagador costuma ser COMAGRO; o recebedor é o nome seguinte "de pessoa/empresa"
            pagador_idx = None
            for i, c in enumerate(candidatos):
                if "COMAGRO" in c.upper():
                    pagador_idx = i
                    break
            if pagador_idx is not None:
                # Após o pagador: CNPJ, BANCO INTER, conta, agência, depois o recebedor
                for c in candidatos[pagador_idx + 1:]:
                    cu = c.upper()
                    if "BANCO INTER" in cu or "BCO " in cu or "ITAÚ" in cu or "ITAU" in cu:
                        continue
                    if "BRADESCO" in cu or "UNIBANCO" in cu:
                        continue
                    if re.match(r'^\d{2}\.\d{3}\.\d{3}/', c):
                        continue
                    if re.fullmatch(r'\*+\d{3}\.\d{3}-\*+', c.replace(" ", "")):
                        continue
                    # Nome do recebedor (pode vir com CPF na mesma linha)
                    nome = re.sub(r'\s+\d{11}\s*$', '', c).strip()
                    nome = re.split(r'\s+\d{2}\.\d{3}\.\d{3}/', nome)[0].strip()
                    if nome and not re.fullmatch(r'[\d.\-]+', nome):
                        dados["nome_recebedor"] = nome
                        break

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

    def _extrair_texto_pdf(self, caminho_arq):
        """Extrai texto de todas as páginas; se for Inter 'oco', complementa com OCR."""
        with pdfplumber.open(caminho_arq) as pdf:
            num_paginas = len(pdf.pages)
            texto_completo = ""
            for page in pdf.pages:
                # Em PDFs imagem (Print To PDF), extract_text pode retornar None
                page_text = page.extract_text() or ""
                texto_completo += page_text + "\n"

        if self._texto_precisa_ocr(texto_completo):
            self.log(f"  OCR necessario: {os.path.basename(caminho_arq)} (Inter sem texto util)")
            ocr = self._ocr_primeira_pagina(caminho_arq)
            if ocr:
                # OCR substitui o texto nativo: no Inter os valores vêm como curva/outline,
                # e os labels nativos ainda têm "Ouvidoria:" que cortaria o OCR no rodapé.
                texto_completo = ocr
            else:
                self.log(
                    "  AVISO: OCR falhou — comprovante Inter vai sair como PGTO generico. "
                    "No Windows: pip install rapidocr-onnxruntime"
                )

        return texto_completo, num_paginas

    def executar(self):
        arquivos = self.selecionar_arquivos()
        if not arquivos:
            return

        sucessos = 0
        erros = 0
        n_regras = len(self.config.get("regras", []))
        caminho_cfg = self.config.get("_caminho_carregado", r"\\Servidor\...\regras_renomeador.json")
        self.log(f"Iniciando... regras={n_regras} grupos | {caminho_cfg}")
        if n_regras == 0:
            self.log("AVISO: JSON de regras vazio ou nao carregou — tudo vai sair como PGTO.")

        for caminho_arq in arquivos:
            try:
                texto_completo, num_paginas = self._extrair_texto_pdf(caminho_arq)

                # Verifica se é DDA / consolidado (caso especial)
                # Tenta primeiro o BB, depois o Itaú (inclui caso de PDF imagem).
                nome_dda = (
                    self.processar_bb_dda(texto_completo)
                    or self.processar_itau_dda(texto_completo, caminho_arq, num_paginas)
                )

                pasta = os.path.dirname(caminho_arq)

                if nome_dda:
                    novo_nome = nome_dda + ".pdf"
                else:
                    dados = self.extrair_dados(texto_completo)
                    novo_nome = self.gerar_novo_nome(dados, os.path.basename(caminho_arq))
                    # Log curto do que as regras/OCR enxergaram (ajuda a debugar PGTO generico)
                    self.log(
                        f"  dados: desc={dados.get('descricao')!r} "
                        f"recebedor={dados.get('nome_recebedor')!r} "
                        f"data={dados.get('data_pgto')!r}"
                    )

                novo_caminho = os.path.join(pasta, novo_nome)

                # Evita sobrescrever se já existir nome igual
                if os.path.abspath(caminho_arq) != os.path.abspath(novo_caminho):
                    if os.path.exists(novo_caminho):
                        base, ext = os.path.splitext(novo_nome)
                        i = 2
                        while os.path.exists(os.path.join(pasta, f"{base}_{i}{ext}")):
                            i += 1
                        novo_nome = f"{base}_{i}{ext}"
                        novo_caminho = os.path.join(pasta, novo_nome)
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
