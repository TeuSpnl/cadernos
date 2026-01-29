import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv

load_dotenv()

# Configurações
XFIN_EMAIL = os.getenv('XFIN_USER')  # Adicione ao seu .env
XFIN_PASS = os.getenv('XFIN_PASS')  # Adicione ao seu .env

# URLs
BASE_URL = "https://app.xfin.com.br"
URL_IMPORTACAO = f"{BASE_URL}/Titulo/Importacao?tipo=1"
URL_LOGIN_PARTIAL = "Login"
URL_ESCOLHA_FILIAL = f"{BASE_URL}/Identity/Account/EscolheFilial"


def get_driver():
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless") # Descomente para rodar em background
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--start-maximized")

    # Ignorar erros de certificado e logs inúteis
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--log-level=3")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver


def fazer_login(driver):
    """Realiza o login se estiver na tela de login."""
    if not XFIN_EMAIL or not XFIN_PASS:
        print("ERRO: Credenciais XFIN_USER ou XFIN_PASS não encontradas no arquivo .env")
        return False

    # 1. Preencher Usuário/Email
    # Usa ID específico encontrado no HTML: id="Input_Email"
    try:
        # Verifica se estamos na tela de login
        if URL_LOGIN_PARTIAL.lower() not in driver.current_url.lower():
            return True  # Já estamos logados ou em outra tela

        email_elem = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "Input_Email"))
        )
        email_elem.clear()
        email_elem.send_keys(XFIN_EMAIL)
    except Exception as e:
        print(f"Erro ao encontrar campo de email: {e}")
        return False

    # 2. Preencher Senha
    # Usa ID específico encontrado no HTML: id="Input_Password"
    try:
        pass_elem = driver.find_element(By.ID, "Input_Password")
        pass_elem.clear()
        pass_elem.send_keys(XFIN_PASS)
    except Exception as e:
        print(f"Erro ao encontrar campo de senha: {e}")
        return False

    # 3. Clicar em Entrar/Login
    try:
        # Procura botão de submit ou botão com texto "Entrar"/"Login"
        btn_login = driver.find_element(By.CSS_SELECTOR, "button[type='submit'], input[type='submit']")
        btn_login.click()
    except Exception as e:
        print(f"Erro ao clicar no botão de login: {e}")
        return False

    # 4. Esperar a "Segunda Tela" (Home/Módulos)
    # Espera a URL mudar e não conter mais "Login"
    try:
        WebDriverWait(driver, 20).until(
            lambda d: URL_LOGIN_PARTIAL.lower() not in d.current_url.lower()
        )
        print("Login realizado com sucesso. Tela inicial carregada.")
        return True
    except Exception:
        print("Timeout aguardando redirecionamento pós-login.")
        return False


def obter_filiais(driver):
    """Vai para a tela de escolha e retorna uma lista de dicts {id, nome} das filiais."""
    print("Obtendo lista de filiais...")
    try:
        driver.get(URL_ESCOLHA_FILIAL)

        # Se cair no login, loga e volta
        if URL_LOGIN_PARTIAL.lower() in driver.current_url.lower():
            if not fazer_login(driver):
                return []
            driver.get(URL_ESCOLHA_FILIAL)

        # Aguarda o select carregar
        select_elem = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "Input_IdFilial"))
        )

        select = Select(select_elem)
        filiais = []

        for option in select.options:
            val = option.get_attribute("value")
            text = option.text
            if val:
                filiais.append({"id": val, "nome": text})

        print(f"Filiais encontradas: {len(filiais)}")
        return filiais

    except Exception as e:
        print(f"Erro ao obter filiais: {e}")
        return []


def selecionar_filial(driver, filial_id):
    """Seleciona uma filial específica na tela de escolha."""
    try:
        # Garante que estamos na tela certa
        if "EscolheFilial" not in driver.current_url:
            driver.get(URL_ESCOLHA_FILIAL)

        select_elem = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "Input_IdFilial"))
        )

        select = Select(select_elem)
        select.select_by_value(filial_id)

        # Clica em Escolher
        btn_escolher = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
        btn_escolher.click()

        # Aguarda sair da tela de escolha (pode ir para Home ou Acesso Negado)
        WebDriverWait(driver, 10).until(
            lambda d: "EscolheFilial" not in d.current_url
        )
        return True
    except Exception as e:
        print(f"Erro ao selecionar filial {filial_id}: {e}")
        return False


def realizar_upload(driver, caminho_arquivo):
    """Navega para a importação e envia o arquivo."""
    try:
        print(f"Acessando tela de importação: {URL_IMPORTACAO}")
        driver.get(URL_IMPORTACAO)

        # Verifica se caiu em Acesso Negado ou Login novamente
        while "AcessoNegado" in driver.current_url or "AccessDenied" in driver.current_url:
            print("Acesso Negado detectado. Tentando recarregar a página de importação...")
            driver.get(URL_IMPORTACAO)

        # Input de arquivo
        file_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "file"))
        )

        abs_path = os.path.abspath(caminho_arquivo)
        file_input.send_keys(abs_path)
        print(f"Arquivo anexado: {abs_path}")

        # Botão Importar
        # O botão pode estar desabilitado inicialmente ou demorar para aparecer
        btn_importar = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "btnImportar"))
        )

        # Clica no botão (usando JS as vezes é mais seguro se tiver sobreposição)
        driver.execute_script("arguments[0].click();", btn_importar)
        print("Botão 'Importar títulos' clicado.")

        # Aguardar feedback de sucesso
        # O sistema usa toastr.success ou exibe um alerta
        try:
            # Espera genérica para processamento (ajustar conforme velocidade do sistema)
            # Procura por toast-success ou alerta de erro
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CLASS_NAME, "toast-success"))
            )
            print("Sucesso: Mensagem de confirmação detectada.")
            return True
        except:
            # Se não apareceu toast de sucesso, verifica se apareceu erro
            print("Aviso: Não detectou toast de sucesso. Verificando erros...")
            try:
                erro = driver.find_element(By.ID, "bannerErros")
                if erro.is_displayed():
                    print("ERRO na importação: Banner de erros exibido.")
                    return False
            except:
                pass

            # Se não deu erro explícito, assume sucesso ou timeout
            print("Upload finalizado (sem confirmação visual explícita capturada).")
            return True

    except Exception as e:
        print(f"Erro durante o upload: {e}")
        return False


def upload_arquivo_xfin(caminho_arquivo):
    if not caminho_arquivo or not os.path.exists(caminho_arquivo):
        print(f"Arquivo não encontrado para upload: {caminho_arquivo}")
        return False

    driver = get_driver()
    sucesso = False

    try:
        # 1. Tenta acessar direto a escolha de filial para ver se pede login
        driver.get(URL_ESCOLHA_FILIAL)

        if URL_LOGIN_PARTIAL.lower() in driver.current_url.lower():
            if not fazer_login(driver):
                driver.quit()
                return False

        # 2. Obtém lista de filiais
        filiais = obter_filiais(driver)

        if not filiais:
            print("Nenhuma filial encontrada ou erro ao listar.")
            driver.quit()
            return False

        # 3. Itera sobre cada filial
        for filial in filiais:
            print(f"\n--- Processando Filial: {filial['nome']} (ID: {filial['id']}) ---")

            # Seleciona a filial
            if selecionar_filial(driver, filial['id']):
                # Faz o upload
                if realizar_upload(driver, caminho_arquivo):
                    print(f"Upload concluído para {filial['nome']}")
                else:
                    print(f"Falha no upload para {filial['nome']}")
                    sucesso_geral = False
            else:
                print(f"Falha ao selecionar filial {filial['nome']}")
                sucesso_geral = False

            # Pequena pausa antes da próxima
            time.sleep(2)

    except Exception as e:
        print(f"Erro geral no Selenium: {e}")
    finally:
        driver.quit()

    return sucesso


if __name__ == "__main__":
    # Teste isolado
    upload_arquivo_xfin("./arquivos/importacao_xfin_oficina_PRONTO - cópia.csv")
