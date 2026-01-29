import pandas as pd

# Dados consolidados conforme nossa longa conversa
data_final = [
    # GRUPO 1 - RECEITAS
    ["1.1", "Venda de produtos", "Receitas", "Vendas de Mercadorias", "Receita Bruta Fiscal."],
    ["1.2", "Prestação de serviços", "Receitas", "Vendas de Serviços", "Receita Bruta Fiscal."],
    
    # GRUPO 2 - IMPOSTOS SOBRE VENDAS
    ["2.1", "DAS - Simples Nacional", "Deduções de Venda", "Simples Nacional", "Guia mensal (DAS)."],
    ["2.2", "PIS", "Deduções de Venda", "PIS", "Se pagar separado do Simples."],
    ["2.3", "COFINS", "Deduções de Venda", "COFINS", "Se pagar separado do Simples."],
    ["2.4", "ISS", "Deduções de Venda", "ISS", "Imposto sobre serviço."],
    ["2.5", "IPI", "Deduções de Venda", "IPI", "Imposto sobre indústria."],
    ["2.6", "ICMS", "Deduções de Venda", "ICMS (Venda)", "Apenas o da SUA nota fiscal. Não lançar compra aqui."],

    # GRUPO 3 - OUTRAS DEDUÇÕES
    ["3.1", "Devoluções de clientes", "Deduções de Venda", "Devolução de Mercadoria Vendida, Garantias", "Estorno de venda."],
    ["3.2", "Taxa de máquina de cartão", "Deduções de Venda", "Taxas de Cartão, Antecipação, Aluguel Maquininha", "Dinheiro retido pela operadora."],
    ["3.4", "Comissões para vendedores", "Deduções de Venda", "Comissão sobre Vendas", "Valor pago ao vendedor."],
    ["3.5", "Fretes e Entregas", "Deduções de Venda", "Fretes s/ Vendas, Motoboy (Entrega), Logística de Venda", "[NOVO] Custo para entregar ao cliente."],

    # GRUPO 4 - CUSTOS VARIÁVEIS
    ["4.1", "Mercadoria para revenda", "Custos Variáveis", "Fornecedores, Frete s/ Compra, ICMS s/ Compra", "Custo de aquisição do estoque."],
    ["4.2", "Matéria-prima", "Custos Variáveis", "Matéria-prima", "Se houver produção."],
    ["4.3", "Insumos", "Custos Variáveis", "Óleos, Graxas, Embalagens, Materiais Diretos", "Consumido na oficina/produção."],
    ["4.4", "Mão de obra variável", "Custos Variáveis", "Produtividade (Oficina)", "Valor pago por serviço feito."],
    ["4.5", "Combustíveis e Logística", "Custos Variáveis", "Combustível, Manutenção de Veículos (Frota)", "[NOVO] Custo de rodar a frota própria."],
    ["4.6", "Manutenção de Equipamentos", "Custos Variáveis", "Manut. Maquinários (Retro, etc), Peças de Reparo", "[NOVO] Manutenção do ativo que gera receita."],

    # GRUPO 5 - GASTOS COM PESSOAL (Custos Fixos)
    ["5.1", "Pró-Labore", "Custos Fixos", "Pró-Labore dos Sócios", "Somente a retirada oficial (com guia INSS). Se for 'por fora', ver 13.3."],
    ["5.2", "Encargos sociais e trabalhistas", "Custos Fixos", "INSS, FGTS, Rescisões, Multas FGTS, Guias Sindicais", "Inclui INSS Patronal sobre Pró-labore."],
    ["5.3", "Salário", "Custos Fixos", "Salário Mensal, 13º Salário, Adiantamentos, Estagiários", "Folha de pagamento líquida."],
    ["5.4", "Transporte", "Custos Fixos", "Vale Transporte", "Deslocamento casa-trabalho."],
    ["5.5", "Alimentação", "Custos Fixos", "Vale Refeição, Lanches (Funcionários), Copa", "Alimentação da equipe no dia a dia."],
    ["5.6", "Saúde", "Custos Fixos", "Plano de Saúde, Exames, Medicina Trabalho, Seguro Vida", "Benefícios de saúde."],
    ["5.7", "Férias", "Custos Fixos", "Pagamento de Férias + 1/3", "[NOVO] Separar do salário para análise."],
    ["5.8", "Uniformes e EPIs", "Custos Fixos", "Fardamentos, Botas, EPIs", "[NOVO] Compra de material de proteção."],
    ["5.9", "Treinamentos", "Custos Fixos", "Cursos, Capacitações", "[NOVO] Investimento em educação da equipe."],

    # GRUPO 6 - GASTOS COM OCUPAÇÃO (Custos Fixos)
    ["6.1", "Água", "Custos Fixos", "Conta de Água/Esgoto", "Infraestrutura."],
    ["6.2", "Aluguel, condomínio, IPTU", "Custos Fixos", "Aluguel Imóvel, IPTU, Seguro Predial", "Custo do imóvel."],
    ["6.3", "Telefone + internet", "Custos Fixos", "Telefone Fixo/Móvel, Internet, Domínios", "Comunicação básica."],
    ["6.4", "Limpeza e conservação", "Custos Fixos", "Material Limpeza, Reparos Prediais, Extintores", "Manutenção do prédio."],
    ["6.5", "Energia elétrica", "Custos Fixos", "Conta de Luz", "Infraestrutura."],

    # GRUPO 7 - SERVIÇOS DE TERCEIROS (Custos Fixos)
    ["7.1", "Contabilidade", "Custos Fixos", "Honorários Contábeis", "Mensalidade do contador."],
    ["7.2", "Serviços jurídicos", "Custos Fixos", "Honorários Jurídicos, Advogados", "Serviços legais."],
    ["7.3", "Consultoria", "Custos Fixos", "Consultorias, Serviços de Terceiros (Geral)", "Serviços intelectuais."],
    ["7.5", "Proteção ao Crédito", "Custos Fixos", "Serasa, SPC, Consultas de Crédito", "[NOVO] Custo de análise de cliente."],

    # GRUPO 8 - MARKETING (Custos Fixos)
    ["8.1", "Anúncios", "Custos Fixos", "Anúncios (Jornal/Revista)", "Mídia offline."],
    ["8.2", "Propaganda", "Custos Fixos", "Google Ads, Facebook Ads, Panfletos", "Mídia online/impressa focada em venda."],
    ["8.3", "Campanhas", "Custos Fixos", "Brindes, Cortesias", "Marketing promocional."],
    ["8.4", "Eventos e Relacionamento", "Custos Fixos", "Confraternizações, Lanches (Clientes), Feiras, São João", "[NOVO] Eventos para clientes e networking."],
    ["8.5", "Viagens e Representação", "Custos Fixos", "Hospedagem, Passagens (Comercial)", "[NOVO] Deslocamento para vendas/negócios."],

    # GRUPO 9 - RECEITAS NÃO OPERACIONAIS
    ["9.1", "Juros de aplicação", "Receita Não-Op.", "Rendimentos de Aplicação", "Lucro sobre investimentos."],
    ["9.2", "Outras receitas não operacionais", "Receita Não-Op.", "Descontos Obtidos, Venda Imobilizado, Indenizações", "Ganhos extras."],
    ["9.3", "Juros sobre Vendas", "Receita Não-Op.", "Juros/Multas recebidos de clientes", "[NOVO] Receita financeira de cobrança."],

    # GRUPO 10 - GASTOS NÃO OPERACIONAIS
    ["10.1", "Juros por atraso", "Despesa Não-Op.", "Juros Pagos, Multas (Boleto/Imposto)", "Custo da ineficiência/atraso."],
    ["10.2", "Tarifas bancárias", "Despesa Não-Op.", "Tarifas Conta, Taxa Cheque Devolvido, Emissão Boleto", "Custo bancário administrativo."],
    ["10.3", "Outros gastos não operacionais", "Despesa Não-Op.", "Multas Diversas, Diferença de Caixa, IOF, Doações", "Perdas diversas e taxas financeiras (IOF)."],
    ["10.4", "Juros sobre Empréstimos", "Despesa Não-Op.", "Juros de Financiamento/Empréstimo", "[NOVO] Custo do capital (Separar do principal)."],

    # GRUPO 11 - IMPOSTO DE RENDA
    ["11.1", "IRPJ", "Impostos Lucro", "IRPJ", "Imposto trimestral/anual."],
    ["11.2", "CSLL", "Impostos Lucro", "CSLL", "Imposto trimestral/anual."],

    # GRUPO 12 - INVESTIMENTOS (Saída de Caixa)
    ["12.1", "Investimentos gerais", "Investimentos", "Compra de Veículos, Equipamentos Grandes", "Bens duráveis."],
    ["12.2", "Obras e Instalações", "Investimentos", "Reformas (Ampliação), Mudança", "[NOVO] Melhorias no imóvel."],
    ["12.3", "Móveis e Utensílios", "Investimentos", "Compra de Móveis, Ar Condicionado, Fogão", "[NOVO] Mobília e eletros duráveis."],

    # GRUPO 13 - TRANSFERÊNCIAS (SAÍDAS) - NÃO É DESPESA
    ["13.1", "Transferências Efetuadas", "Transferência", "Aplicação Financeira (Principal), Pagto Empréstimo (Principal)", "Dinheiro saindo do caixa, mas não é despesa."],
    ["13.3", "Antecipação de Lucros", "Transferência", "Retiradas (Vales), Retirada Fixa (s/ guia)", "[NOVO] Dinheiro para o sócio (Isento). Analisar se não é Pró-labore."],
    ["13.4", "Contas Correntes Sócios", "Transferência", "Pagamento Contas Particulares (Água/Luz do Sócio)", "[NOVO] Mistura de patrimônio (controlar aqui)."],
    ["13.5", "Pagamento de Parcelamentos", "Transferência", "REFIS, Parcelamentos Tributários", "[NOVO] Pagamento de dívida antiga."],

    # GRUPO 14 - TRANSFERÊNCIAS (ENTRADAS) - NÃO É RECEITA
    ["14.1", "Transferências Recebidas", "Transferência", "Entrada de Empréstimo, Resgate Aplicação (Principal)", "Dinheiro entrando, mas não é venda."],

    # GRUPO 15 - DESPESAS ADMINISTRATIVAS [NOVO GRUPO]
    ["15.1", "Tecnologia e Sistemas", "Custos Fixos", "Licenças Software, Certificado Digital, Sistema Ponto", "[NOVO] TI e Sistemas."],
    ["15.2", "Material de Uso e Consumo", "Custos Fixos", "Mat. Escritório, Limpeza, Bens Peq. Valor (Capinhas)", "[NOVO] Consumo do dia a dia."],
    ["15.3", "Correios e Entregas (Adm)", "Custos Fixos", "Correios, Malotes, Fretes Administrativos", "[NOVO] Logística de documentos."],
    ["15.4", "Taxas e Legalização", "Custos Fixos", "Alvarás, Taxas Cartoriais, Emolumentos Judiciais", "[NOVO] Burocracia estatal/legal."]
]

# Criar DataFrame
df_mapa = pd.DataFrame(data_final, columns=["Código Xfin", "Nome da Conta", "Categoria", "O que lançar aqui (Seculos)", "Observação Importante"])

# Exportar para Excel
file_name = "arquivos/De_Para_Seculos_Xfin_Final.xlsx"
df_mapa.to_excel(file_name, index=False)

print(f"Arquivo '{file_name}' gerado com sucesso.")
print(df_mapa.head(10)) # Mostrar prévia
print(df_mapa.tail(10)) # Mostrar prévia final