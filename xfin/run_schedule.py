# run_schedule.py
import schedule
import time
import pagto_sec_p_xfin
import xfin_uploader
import email_alert
import os

def executar_sincronizacao():
    print("\n--- Iniciando Sincronização Automática ---")
    
    arquivo_cod = "arquivos\\[XFIN] Plano de contas para o xfin.xlsx"
    arquivo_desc = "arquivos\\[XFIN] Descrição contas xfin.xlsx"
    
    if not os.path.exists(arquivo_cod) or not os.path.exists(arquivo_desc):
        print("ERRO: Arquivos de mapeamento não encontrados. Parando execução.")
        return
    
    # 1. Inicializa DB se necessário
    pagto_sec_p_xfin.inicializar_db_controle()
    
    # 2. Gera o arquivo CSV e salva IDs no SQLite
    arquivos_csv = pagto_sec_p_xfin.main()
    
    # 3. Se gerou arquivo, faz o upload
    if arquivos_csv:
        print(f"Arquivo gerado: {arquivos_csv}. Iniciando upload...")
        sucesso_upload = xfin_uploader.upload_arquivo_xfin(arquivos_csv)
        
        if sucesso_upload:
            print(f"Ciclo concluído para os {arquivos_csv}: Enviados com sucesso.")
        else:
            print(f"ERRO: Falha no upload dos {arquivos_csv}.")
    else:
        print("Nenhum arquivo gerado (sem dados novos ou erro no processamento).")

print("--- Agendador Xfin Iniciado ---")
print("Horários programados: 12:00 e 18:30")

# Agenda as execuções
schedule.every().day.at("12:00").do(executar_sincronizacao)
schedule.every().day.at("12:48").do(executar_sincronizacao)
schedule.every().day.at("18:30").do(executar_sincronizacao)

# Loop infinito
while True:
    schedule.run_pending()
    time.sleep(60) # Verifica a cada minuto