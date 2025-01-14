# testar.py
import subprocess
import sys

def main():
    # pool_sizes que você deseja testar
    pool_sizes = [5, 10, 15, 20]

    # As 4 passadas de threads que você quer:
    #  1) 5->20
    #  2) 20->5
    #  3) 5->20
    #  4) 20->5
    passadas = [
        range(5, 21),       # 1ª: 5 a 20 (inclusive)
        range(20, 4, -1),   # 2ª: 20 a 5 (decrescente)
        range(5, 21),       # 3ª: 5 a 20
        range(20, 4, -1)    # 4ª: 20 a 5
    ]

    # Vamos rodar para cada pool_size
    for ps in pool_sizes:
        for idx_passada, pass_range in enumerate(passadas, start=1):
            for mw in pass_range:
                print(f"\n=== pool_size={ps}, passada={idx_passada}, threads={mw} ===\n")
                
                # Montar a linha de comando para rodar "faturamento.py"
                # Passamos pool_size, max_workers e idx_passada só para log, se quiser
                cmd = [
                    sys.executable,      # python atual
                    "./Dynamo/faturamento.py",    # seu script de faturamento
                    "--poolsize", str(ps),
                    "--threads", str(mw),
                    "--passada", str(idx_passada)
                ]

                # Executar e esperar terminar
                result = subprocess.run(cmd)
                if result.returncode != 0:
                    print(f"** ERRO com pool_size={ps}, passada={idx_passada}, threads={mw}, code={result.returncode}. Abortando. **")
                    return
    
    print("Todos os testes concluídos com sucesso.")

if __name__ == "__main__":
    main()