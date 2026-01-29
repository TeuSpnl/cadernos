# run_schedule.py
import schedule
import time
from email_alert import executar_sincronizacao

print("--- Agendador Xfin Iniciado ---")
print("Horários programados: 12:00 e 18:30")

# Agenda as execuções
schedule.every().day.at("12:00").do(executar_sincronizacao)
schedule.every().day.at("18:30").do(executar_sincronizacao)

# Loop infinito
while True:
    schedule.run_pending()
    time.sleep(60) # Verifica a cada minuto