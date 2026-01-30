# email_alert.py
from email.mime.multipart import MIMEMultipart
from email.message import EmailMessage
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from dotenv import load_dotenv
from email import encoders
import smtplib
import ssl
import base64
import msal
import os

load_dotenv()

# Carrega variáveis de ambiente
client_secret = os.getenv('client_secret')
client_id = os.getenv('client_id')
tenant_id = os.getenv('tenant_id')


# Configurações fixas
EMAIL_ADDRESS = 'noreply-faturas@comagro.com.br'
EMAIL_PASS = os.getenv('senha_email')
DESTINATARIO_ERRO = 'errosxfin@comagro.com.br'


def encode_oauth2_string(username, access_token):
    auth_string = f"user={username}\x01auth=Bearer {access_token}\x01\x01"
    return auth_string


def get_oauth2_token():
    # URL de token do Azure Entra
    url = f"https://login.microsoftonline.com/{tenant_id}"

    # Escopo para enviar email
    scope = ['https://graph.microsoft.com/.default']

    # Autenticação usando MSAL
    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        authority=url,
        client_credential=client_secret
    )

    # Obtém o token de acesso
    token_response = app.acquire_token_for_client(scopes=scope)

    if 'access_token' in token_response:
        return token_response['access_token']
    else:
        return None


def enviar_email_erro(arquivo_csv_erro, qtd_erros):
    """
    Envia um e-mail notificando erros de mapeamento, com o CSV em anexo.
    """
    print(f"Preparando envio de e-mail para {DESTINATARIO_ERRO}...")

    msg = MIMEMultipart()
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = DESTINATARIO_ERRO
    msg['Subject'] = f"[ALERTA XFIN] {qtd_erros} Contas sem Plano de Contas identificado"

    body = f"""
    <h3>Alerta de Importação</h3>
    <p>O sistema identificou <b>{qtd_erros}</b> contas a pagar vindas do Seculos que não possuem correspondência no mapa do Xfin.</p>
    <p>Por favor, analise o arquivo em anexo, atualize o 'Plano de contas para o xfin.xlsx' e rode a importação novamente.</p>
    <p>Link para envio: app.xfin.com.br/Titulo/Importacao?tipo=1</p>
    <p><i>Atenciosamente,<br>Seu Robô Financeiro</i></p>
    """
    msg.attach(MIMEText(body, 'html'))

    # Anexa o arquivo CSV
    try:
        with open(arquivo_csv_erro, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())

        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {arquivo_csv_erro.split('/')[-1]}",
        )
        msg.attach(part)
    except Exception as e:
        print(f"Erro ao anexar arquivo: {e}")
        return

    # Envio via SMTP com OAuth2
    try:
        access_token = get_oauth2_token()

        if access_token is None:
            print("Não foi possível obter o token de acesso OAuth2.")
            return False
        
        smtp = smtplib.SMTP('smtp.office365.com', 587)
        smtp.ehlo()
        smtp.starttls(context=ssl.create_default_context())  # Inicia a conexão TLS
        smtp.ehlo()
        smtp.login(EMAIL_ADDRESS, EMAIL_PASS)
        
        auth_string = encode_oauth2_string(EMAIL_ADDRESS, access_token)
        
        try:
            smtp.docmd('AUTH XOAUTH2', auth_string)
        except smtplib.SMTPException as e:
            print("Erro de Autenticação! " + f"[Erro]: {str(e)}\nNão foi possível autenticar usando OAuth2.")
        
        try:             
            smtp.sendmail(EMAIL_ADDRESS, DESTINATARIO_ERRO, msg.as_string())
        except Exception as e:
            print(f"Erro ao enviar e-mail: {e}")
            return

        print("E-mail de erro enviado com sucesso!")
    except Exception as e:
        print(f"ERRO CRÍTICO ao enviar e-mail: {e}")
