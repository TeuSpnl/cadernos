import pandas as pd

# Dados consolidados baseados em toda a conversa
data_final = [
    # GRUPO 1 - RECEITAS
    ["1.1", "Venda de px≈rodutos", "Receita com Vendas", "Vendas de Mercadorias", ""],
    ["1.2", "Prestação de serviços", "Receita com Vendas", "Vendas de Serviços", ""],

    # GRUPO 2 - IMPOSTOS SOBRE VENDAS
    ["2.1", "DAS - Simples Nacional", "Impostos s/ Vendas", "Simples Nacional", "Imposto mensal da guia DAS"],
    ["2.2", "PIS", "Impostos s/ Vendas", "PIS", "Se pago separado"],
    ["2.3", "COFINS", "Impostos s/ Vendas", "COFINS", "Se pago separado"],
    ["2.4", "ISS", "Impostos s/ Vendas", "ISS", "Imposto sobre Nota de Serviço"],
    ["2.5", "IPI", "Impostos s/ Vendas", "IPI", ""],
    ["2.6", "ICMS", "Impostos s/ Vendas", "ICMS (apenas sobre Venda)", "NÃO lançar ICMS de compra/DIFAL aqui"],

    # GRUPO 3 - DEDUÇÕES
    ["3.1", "Devoluções de clientes", "Outras Deduções", "Devolução de Vendas, Garantias (Peças)", "Reduz o faturamento bruto"],
    ["3.2", "Taxa de máquina de cartão", "Outras Deduções", "Taxas de Cartão, Aluguel Maquininha, Antecipação", "Dinheiro que a operadora comeu"],
    ["3.4", "Comissões para vendedores", "Outras Deduções", "Comissões s/ Vendas", ""],
    ["3.5", "Fretes e Entregas (Vendas)", "Outras Deduções", "Fretes s/ Vendas, Motoboy de Entrega", "[NOVO] Custo logístico da venda"],

    # GRUPO 4 - CUSTOS VARIÁVEIS
    ["4.1", "Mercadoria para revenda", "Custos Variáveis", "Fornecedores, Fretes s/ Compras, ICMS s/ Compras", "Custo de aquisição do estoque"],
    ["4.2", "Matéria-prima", "Custos Variáveis", "Matéria-prima (se houver produção)", ""],
    ["4.3", "Insumos", "Custos Variáveis", "Óleos, Graxas, Estopas, Embalagens", "Gasto direto na oficina/produto"],
    ["4.4", "Mão de obra variável", "Custos Variáveis", "Produtividade da Oficina", "Pago por peça/serviço feito"],
    ["4.5", "Combustíveis e Logística", "Custos Variáveis", "Diesel Retro, Gasolina Entrega, Manut. Frota", "[NOVO] Veículos de trabalho"],
    ["4.6", "Manutenção de Equipamentos", "Custos Variáveis", "Peças Retro, Manut. Maquinário", "[NOVO] Manter a operação rodando"],

    # GRUPO 5 - PESSOAL
    ["5.1", "Pró-Labore", "Gastos com Pessoal", "Pró-labore Oficial (Guias)", "ANALISAR: Se for 'retirada/vale' sem guia, vai p/ 13.3"],
    ["5.2", "Encargos Sociais e Trab.", "Gastos com Pessoal", "INSS, FGTS, Rescisões, Sindicato, IRRF (Guia)", "Encargos s/ Pró-labore entram aqui"],
    ["5.3", "Salário", "Gastos com Pessoal", "Salário Mensal, 13º, Adiantamentos, Estágio", ""],
    ["5.4", "Transporte", "Gastos com Pessoal", "Vale Transporte", "Apenas deslocamento funcionário"],
    ["5.5", "Alimentação", "Gastos com Pessoal", "Vale Refeição, Lanches Equipe, Copa", "Benefício de comida"],
    ["5.6", "Saúde", "Gastos com Pessoal", "Plano de Saúde, Exames, Seguros de Vida", ""],
    ["5.7", "Férias", "Gastos com Pessoal", "Pagamento de Férias + 1/3", "[NOVO] Separar do salário mensal"],
    ["5.8", "Uniformes e EPIs", "Gastos com Pessoal", "Fardamentos, Botas, EPIs", "[NOVO] Proteção e Vestimenta"],
    ["5.9", "Treinamentos", "Gastos com Pessoal", "Cursos, Capacitação", "[NOVO] Investimento em gente"],

    # GRUPO 6 - OCUPAÇÃO
    ["6.1", "Água", "Gastos com Ocupação", "Conta de Água/Esgoto", ""],
    ["6.2", "Aluguel, Condomínio, IPTU", "Gastos com Ocupação", "Aluguel Imóveis, IPTU", "Seguro Incêndio entra aqui"],
    ["6.3", "Telefone + internet", "Gastos com Ocupação", "Telefonia Fixa, Internet, Celular Corporativo", ""],
    ["6.4", "Limpeza e conservação", "Gastos com Ocupação", "Material Limpeza Predial, Reparos, Extintores", "Manutenção do prédio (não máquinas)"],
    ["6.5", "Energia elétrica", "Gastos com Ocupação", "Conta de Luz", ""],

    # GRUPO 7 - TERCEIROS
    ["7.1", "Contabilidade", "Serviços de Terceiros", "Honorários Contábeis", ""],
    ["7.2", "Serviços jurídicos", "Serviços de Terceiros", "Advogados", ""],
    ["7.3", "Consultoria", "Serviços de Terceiros", "Consultorias Diversas", ""],
    ["7.5", "Proteção ao Crédito", "Serviços de Terceiros", "Serasa, SPC, Consultas", "[NOVO] Serviço de análise"],

    # GRUPO 8 - MARKETING
    ["8.1", "Anúncios", "Gastos com Marketing", "Google Ads, Facebook Ads", ""],
    ["8.2", "Propaganda", "Gastos com Marketing", "Panfletos, Rádio, Outdoor, 'Outros Gastos Com.'", ""],
    ["8.3", "Campanhas", "Gastos com Marketing", "Brindes, Cortesias", ""],
    ["8.4", "Eventos e Relacionamento", "Gastos com Marketing", "São João, Aniversário Empresa, Lanches Clientes", "[NOVO] Festas e Mimos"],
    ["8.5", "Viagens e Representação", "Gastos com Marketing", "Hospedagem, Viagem Comercial", "[NOVO] Custo de visita a cliente"],

    # GRUPO 9 - RECEITAS NÃO OPERACIONAIS
    ["9.1", "Juros de aplicação", "Rec. Não Operacionais", "Rendimentos (Valor Líquido)", "Se vier bruto, lance imposto no 10.3"],
    ["9.2", "Outras receitas não op.", "Rec. Não Operacionais", "Descontos Obtidos, Venda de Ativo, Indenizações", "Ganhos extras"],
    ["9.3", "Juros sobre Vendas", "Rec. Não Operacionais", "Juros/Multas cobrados de clientes", "[NOVO] Separar financeiro de venda"],

    # GRUPO 10 - DESPESAS NÃO OPERACIONAIS (10.5 REMOVIDO)
    ["10.1", "Juros por atraso", "Gastos Não Operac.", "Juros de Boletos, Multas Tributárias", "Custo da ineficiência"],
    ["10.2", "Tarifas bancárias", "Gastos Não Operac.", "Cesta, DOC/TED, Tarifa Cheque, Protesto", "Custo do banco"],
    ["10.3", "Outros gastos não op.", "Gastos Não Operac.", "IOF, Diferença de Caixa, Multas Diversas", "Remuneração Extra-Oficial entra aqui"],
    ["10.4", "Juros sobre Empréstimos", "Gastos Não Operac.", "Juros da parcela do financiamento", "[NOVO] Separar do Principal (14.1)"],

    # GRUPO 11 - IMPOSTO DE RENDA
    ["11.1", "IRPJ", "Imposto de Renda", "IRPJ (Trimestral/Anual)", "Só imposto sobre Lucro"],
    ["11.2", "CSLL", "Imposto de Renda", "CSLL", "Só imposto sobre Lucro"],

    # GRUPO 12 - INVESTIMENTOS
    ["12.1", "Investimentos gerais", "Investimentos", "Consórcios, Veículos, Grandes Ativos", ""],
    ["12.2", "Obras e Instalações", "Investimentos", "Reformas de Ampliação, Mudança", "[NOVO]"],
    ["12.3", "Móveis e Utensílios", "Investimentos", "Fogão, Móveis, Ar Condicionado, 'Bens Peq Valor'", "[NOVO] Bens duráveis de escritório"],

    # GRUPO 13 - SAÍDAS (TRANSFERÊNCIAS)
    ["13.1", "Transf. entre contas (Saída)", "Transf. Saída", "Aplicação Financeira (Saída)", "Dinheiro indo investir"],
    ["13.3", "Antecipação de Lucros", "Transf. Saída", "Retiradas Sócios, Vales sem guia", "[NOVO] ANALISAR: Separar do Pró-labore"],
    ["13.4", "Contas Correntes Sócios", "Transf. Saída", "Pagamento conta pessoal do sócio", "[NOVO] ANALISAR: Separar do Pró-labore"],
    ["13.5", "Pagamento de Parcelamentos", "Transf. Saída", "REFIS, Parcelamento INSS/Simples", "[NOVO] Pagamento de dívida velha"],
    
    # GRUPO 14 - ENTRADAS (TRANSFERÊNCIAS)
    ["14.1", "Transf. entre contas (Entrada)", "Transf. Entrada", "Empréstimos, Resgate Aplicação (Principal)", "Entrada de dinheiro que não é venda"],

    # GRUPO 15 - ADMINISTRATIVAS (NOVO GRUPO)
    ["15.1", "Tecnologia e Sistemas", "Desp. Administrativas", "Licenças, Software, Ponto, Certificado Digital", "[NOVO] TI e Sistemas"],
    ["15.2", "Material de Uso e Consumo", "Desp. Administrativas", "Papelaria, Limpeza, Bens Pequenos (Capinha)", "[NOVO] Consumo do dia a dia"],
    ["15.3", "Correios e Entregas (Adm)", "Desp. Administrativas", "Malotes, Documentos", "[NOVO] Não confundir com frete venda"],
    ["15.4", "Taxas e Legalização", "Desp. Administrativas", "Alvará, Cartório, Taxas Fed/Est/Mun", "[NOVO] Burocracia estatal"],
]

df_export = pd.DataFrame(data_final, columns=["Código Xfin", "Nome da Conta", "Categoria (Grupo)", "O que Lançar (Seculos)", "Observação / Regra"])

# Salvar
file_path = "arquivos/plano_contas_consolidado_xfin.xlsx"
df_export.to_excel(file_path, index=False)

print(f"Arquivo gerado: {file_path}")
print(df_export.head(10)) # Mostrar o início para conferência