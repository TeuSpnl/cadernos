import schedule
import time
from faturamento import main  # Importe a função main que deseja agendar

def run_schedule():
    # Agendar a função às 12:00 e 22:00 no timezone definido
    schedule.every().day.at("08:00").do(main)
    print("Scheduler started")

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    try:
        run_schedule()
    except Exception as e:
        print(f"Erro: {e}")
