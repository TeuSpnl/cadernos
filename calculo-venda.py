import os

def main():
    # Limpa a tela pra dar um ar mais profissional
    os.system('cls' if os.name == 'nt' else 'clear')
    
    # Mapa da criptografia "indecifrável"
    cipher_map = {
        'v': '1', 'c': '2', 'o': '3', 'n': '4', 'q': '5',
        'u': '6', 'i': '7', 's': '8', 't': '9', 'a': '0'
    }

    try:
        # Pergunta inicial
        qtd = int(input("Quantas peças vamos processar? "))
    except ValueError:
        print("Eu preciso de um número inteiro. Comece de novo.")
        return

    lista_final = []
    total_geral = 0.0

    print("-" * 30)

    for i in range(qtd):
        while True:
            # Captura o código
            raw_code = input(f"Código criptografado peça {i+1}: ").strip().lower()

            # Decodificação
            decoded_str = ""
            for char in raw_code:
                # Rejeita caracteres inválidos
                if char not in cipher_map and char not in [',', '.']:
                    print("Código inválido. Vamos tentar de novo para a mesma peça (eu tenho o dia todo).")
                    decoded_str = ""
                    break
                if char in cipher_map:
                    decoded_str += cipher_map[char]
                elif char == ',' or char == '.':
                    decoded_str += '.'
            
            # Verificação de sanidade antes de tentar converter
            if not decoded_str:
                print("Se você quiser perder tempo digitando nada, beleza, eu tenho o dia todo.")
                continue

            try:
                # Converte para número e faz o cálculo
                valor_base = float(decoded_str)
                resultado = valor_base / 0.59
                
                # Formatação PT-BR
                res_formatado = f"{resultado:.2f}".replace('.', ',')
                
                # Feedback
                print(f"Resultado: {res_formatado}")
                print("-" * 15)

                # Armazena na lista plana
                lista_final.append(raw_code)
                lista_final.append(res_formatado)
                
                total_geral += resultado
                
                # Se chegou até aqui sem erro, quebramos o while interno 
                # e vamos para a próxima peça do for
                break 

            except ValueError:
                # Mensagem motivacional
                print("Código inválido. Vamos tentar de novo para a mesma peça (eu tenho o dia todo).")

    # Resultados Finais
    print("\n" + "=" * 30)
    # Exibe a lista bruta como string, removendo as aspas para ficar igual ao seu exemplo
    lista_str = ", ".join(lista_final)
    print(f"Lista: [{lista_str}]")
    
    total_formatado = f"{total_geral:.2f}".replace('.', ',')
    print(f"Total: {total_formatado}")
    print("=" * 30)

if __name__ == "__main__":
    main()