import pyautogui as ptgui
import os

from win32api import GetSystemMetrics
import subprocess as sbp
import psutil
from time import sleep
from datetime import datetime, timedelta

# Tamanho da tela
screen_width = GetSystemMetrics(0)
screen_height = GetSystemMetrics(1)


def move_loop(times, key):
    """    Loop de movimentação    """
    for i in range(times):
        ptgui.press(key)
        
def insert_date(date):
    """    Insere a data no campo de datas do sistema    """
    for i in range(3):
        ptgui.write(date[i])

def fechaApp(app_name):
    """    Fecha o sistema para a cobrança funcionar direito    """
    for i in psutil.process_iter():
        if i.name() == app_name:
            sbp.Popen(f"tskill {i.pid}")
        
def openSec():
    """    Abre o sistema Séculos e loga com a conta 'Mateus'    """
    sbp.Popen(["start", "C:\\Micromais\\mmseculos\\Seculos.exe"], shell=True)
    sleep(1.5)

    ptgui.write("mateus")
    ptgui.press("tab")
    ptgui.write("teta12")
    ptgui.press("enter")
    
    # Previne que as páginas iniciais atrapalhem no desenvolvimento do programa
    sleep(.25)
    move_loop(5, "esc")

def update_pdfs():
    """    Atualiza a pasta de PDFs através do sistema séculos    """
    ptgui.hotkey("win", "d")
    sleep(.25)
    
    fechaApp("Seculos.exe")
    sleep(.25)
    openSec()
    
    duplicatas = open("./boletos.txt", "r").read().splitlines()

    # Percorre Movimentações > Cliente > Fatura / Duplicata > Duplicatas / Boleto Bancário
    ptgui.press("alt")
    ptgui.press("m")
    ptgui.press("c")
    ptgui.press("f")
    ptgui.press("d")

    sleep(.15)

    # Entra na seção de Enviados
    move_loop(2, "right")

    # Pega as datas úteis e as insere nos campos de data final e inicial
    ptgui.press("tab")
    today = datetime.today()
    initial = (today + timedelta(days=-10)).strftime('%d/%m/%Y').split('/')
    final = (today + timedelta(days=100)).strftime('%d/%m/%Y').split('/')
    
    sleep(.25)

    insert_date(initial)
    ptgui.press("tab")
    insert_date(final)

    # Clica botão localizar
    ptgui.click(screen_width - 100, 150)
    sleep(1.75)

    for n in duplicatas:
        print(f"Baixando {n}...")
        # Seleciona a duplicata e abre a página de download do PDF
        ptgui.click(70, 215)
        sleep(.5)
        ptgui.write(n)
        ptgui.press("enter")
        sleep(.5)
        ptgui.press("space")
        ptgui.press("apps")
        ptgui.press("up")
        move_loop(2, "enter")
        sleep(.5)

        # Cria a pasta PDF, caso não exista
        if not os.path.isdir("C:\\PDF"):
            os.mkdir("C:\\PDF")

        # Salva o PDF no local correto com o nome correto
        ptgui.click(75, 35)
        sleep(.5)
        ptgui.press("tab")
        ptgui.write(f"C:\PDF\{n.replace('/','-')}.pdf")
        move_loop(2, "enter")
        sleep(.5)

        # Sai da página de salvação e prepara o programa para salvar o próximo PDF
        ptgui.press("esc")
        sleep(.5)
        ptgui.click(70, 215)
        sleep(.2)
        ptgui.click(30, 195)
        sleep(.2)
        ptgui.click(30, 195)
        sleep(.5)
        
        sleep(1.85)


if __name__ == "__main__":
    update_pdfs()