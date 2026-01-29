# email_alert.py
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import msal
import base64
import os
from dotenv import load_dotenv

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


def get_oauth_token():
    app = msal.ConfidentialClientApplication(
        client_id, authority=f"https://login.microsoftonline.com/{tenant_id}",
        client_credential=client_secret
    )
    result = app.acquire_token_for_client(scopes=["https://outlook.office365.com/.default"])
    return result.get('access_token')


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
        token = get_oauth_token()
        auth_string = encode_oauth2_string(EMAIL_ADDRESS, token)

        with smtplib.SMTP('smtp.office365.com', 587) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.docmd('AUTH XOAUTH2', auth_string)
            smtp.sendmail(EMAIL_ADDRESS, DESTINATARIO_ERRO, msg.as_string())

        print("E-mail de erro enviado com sucesso!")
    except Exception as e:
        print(f"ERRO CRÍTICO ao enviar e-mail: {e}")
