import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv

load_dotenv()

# Configurações
XFIN_EMAIL = os.getenv('XFIN_USER') # Adicione ao seu .env
XFIN_PASS = os.getenv('XFIN_PASS')  # Adicione ao seu .env
URL_IMPORTACAO = "https://app.xfin.com.br/Titulo/Importacao?tipo=1"
URL_LOGIN_PARTIAL = "Login" # Parte da URL que identifica a tela de login

def get_driver():
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless") # Descomente para rodar em background
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--start-maximized")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def fazer_login(driver):
    print("Redirecionado para Login. Iniciando autenticação...")
    
    if not XFIN_EMAIL or not XFIN_PASS:
        print("ERRO: Credenciais XFIN_USER ou XFIN_PASS não encontradas no arquivo .env")
        return False

    # 1. Preencher Usuário/Email
    # Usa ID específico encontrado no HTML: id="Input_Email"
    try:
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

def upload_arquivo_xfin(caminho_arquivo):
    if not caminho_arquivo or not os.path.exists(caminho_arquivo):
        print(f"Arquivo não encontrado para upload: {caminho_arquivo}")
        return False

    driver = get_driver()
    sucesso = False

    try:
        print(f"Acessando: {URL_IMPORTACAO}")
        driver.get(URL_IMPORTACAO)
        time.sleep(2) # Breve espera para carregamento

        # Verifica se caiu no Login
        if URL_LOGIN_PARTIAL.lower() in driver.current_url.lower():
            logado = fazer_login(driver)
            if logado:
                print("Redirecionando novamente para a tela de importação...")
                driver.get(URL_IMPORTACAO)
            else:
                print("Falha no processo de login.")
                return False

        # Agora deve estar na tela de importação
        # Procura o input type='file'
        try:
            file_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='file']"))
            )
            
            # Envia o caminho absoluto do arquivo
            abs_path = os.path.abspath(caminho_arquivo)
            file_input.send_keys(abs_path)
            print(f"Arquivo selecionado: {abs_path}")

            # Clicar no botão de Enviar/Importar
            # Geralmente é um botão próximo ao input file. Ajuste o seletor conforme o HTML real.
            # Tentativa genérica: Botão que contenha texto "Importar" ou "Enviar"
            btn_enviar = driver.find_element(By.XPATH, "//button[contains(text(), 'Importar') or contains(text(), 'Enviar') or contains(text(), 'Upload')]")
            btn_enviar.click()
            
            print("Botão de importação clicado. Aguardando processamento...")
            
            # Aguardar mensagem de sucesso ou mudança de estado
            # Aqui depende muito do feedback do sistema Xfin. 
            # Vamos esperar 10 segundos para garantir o envio.
            time.sleep(10) 
            sucesso = True
            print("Processo de upload finalizado.")

        except Exception as e:
            print(f"Erro ao interagir com elementos de upload: {e}")
            sucesso = False

    except Exception as e:
        print(f"Erro geral no Selenium: {e}")
    finally:
        driver.quit()
    
    return sucesso

if __name__ == "__main__":
    # Teste isolado
    upload_arquivo_xfin("./arquivos/importacao_xfin_oficina_PRONTO - cópia.csv")
