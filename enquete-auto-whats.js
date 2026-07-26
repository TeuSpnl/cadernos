async function automatizarEnquetesSupremas() {
    const sleep = ms => new Promise(r => setTimeout(r, ms));

    // Função para gerar pausas imprevisíveis
    const sleepRandom = (min, max) => sleep(Math.floor(Math.random() * (max - min + 1)) + min);


    console.log("Iniciando a Operação Moisés: dividindo as enquetes ao meio...");

    const inserirTexto = async (seletor, texto) => {
        let campo = null;
        
        for(let i = 0; i < 20; i++) {
            campo = document.querySelector(seletor);
            if (campo) break;
            await sleep(100);
        }
        
        if (!campo) return false;

        campo.focus();
        campo.click(); 
        
        const p = campo.querySelector('p') || campo;
        const selection = window.getSelection();
        const range = document.createRange();
        range.selectNodeContents(p);
        range.collapse(false);
        selection.removeAllRanges();
        selection.addRange(range);

        // Hesitação humana antes de começar a "digitar"
        await sleepRandom(200, 600); 
        document.execCommand('insertText', false, texto);
        // Tempo de leitura da interface para renderizar a próxima caixa sem desconfiar
        await sleepRandom(600, 1100); 
        return true;
    };

    // Nossas duas enquetes salvadoras
    const enquetes = [
        {
            titulo: "Disponibilidade Ensaios (Seg a Qui) - Auto da Paixão",
            dias: ["Segunda", "Terça", "Quarta", "Quinta"]
        },
        {
            titulo: "Disponibilidade Ensaios (Sex a Dom) - Auto da Paixão",
            dias: ["Sexta", "Sábado", "Domingo"]
        }
    ];

    const turnos = ["de manhã", "de tarde", "de noite"];

    // Loop que vai criar e enviar cada enquete
    for (let numEnquete = 0; numEnquete < enquetes.length; numEnquete++) {
        const enqueteAtual = enquetes[numEnquete];
        
        // 1. Clicar no botão de anexos (clipe ou +)
        const btnAnexo = document.querySelector('button[aria-label="Allega"]') || document.querySelector('[data-testid="plus-rounded"]')?.closest('button');
        if (!btnAnexo) {
            console.error("Botão de anexos ('Allega') não encontrado. A conversa está aberta?");
            return;
        }
        btnAnexo.click();
        await sleep(700);

        // 2. Clicar na Enquete (Sondaggio)
        const btnSondaggio = document.querySelector('button[aria-label="Sondaggio"]');
        if (!btnSondaggio) {
            console.error("Botão 'Sondaggio' fugiu.");
            return;
        }
        btnSondaggio.click();
        await sleep(1500);

        // 3. Preencher o título
        await inserirTexto('[data-testid="poll-question-input"]', enqueteAtual.titulo);

        // 4. Montar a lista de opções da vez
        const opcoes = [];
        for (const dia of enqueteAtual.dias) {
            for (const turno of turnos) {
                if (dia === "Domingo" && turno === "de noite") continue;
                
                if (dia === "Domingo" && turno === "de manhã") {
                    opcoes.push("Domingo de manhã (após a EBD)");
                } else {
                    opcoes.push(`${dia} ${turno}`);
                }
            }
        }

        // 5. Injetar as opções sem dó
        for (let i = 0; i < opcoes.length; i++) {
            const sucesso = await inserirTexto(`[data-testid="poll-option-input-${i}"]`, opcoes[i]);
            if (!sucesso) {
                console.warn(`Campo para a opção '${opcoes[i]}' não encontrado!`);
            }
        }

        // 6. Enviar a enquete
        await sleepRandom(1000, 2000); // Pausa dramática final antes do clique
        const btnEnviar = document.querySelector('[data-testid="poll-send-button"]');
        if (btnEnviar) {
            btnEnviar.click();
            console.log(`Enquete ${numEnquete + 1} enviada com sucesso!`);
        } else {
            console.error("Botão de enviar sumiu no espaço-tempo.");
        }
        
        // Pausa estratégica para o café entre as duas enquetes (entre 4 e 8 segundos)
        if (numEnquete < enquetes.length - 1) {
            console.log("Aguardando uns segundos para não parecer um bot desesperado...");
            await sleepRandom(4000, 8000); 
        }
    }

    console.log("Automação finalizada. Que venham os ensaios!");
}

automatizarEnquetesSupremas();