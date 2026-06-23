--------------------------------------------------------------------------------
-- Vendingmachine.vhd
-- Maquina de Vender Salgados - Circuitos Digitais (UESB)
-- Placa Altera DE1 / FPGA Cyclone II EP2C20F484C7
--
--   * Unidade de controle (FSM): IDLE -> CHECK_STOCK -> ACCEPT_MONEY ->
--                                DISPENSE_ITEM -> FINAL_STATE (troco)
--                                + NO_STOCK (aviso) e RETURN_MONEY (desistencia)
--
-- POR QUE A VERSAO ANTIGA FALHAVA NA PLACA (e como foi corrigido aqui):
--   1) Somava moeda a CADA borda de clock enquanto o botao estava apertado.
--      -> Agora soma UMA vez por clique (deteccao de borda + debounce).
--   2) Botoes sem debounce/borda -> trepidacao virava varios eventos.
--      -> Agora ha sincronizador + amostragem lenta + pulso de 1 ciclo.
--   3) Saida com dois drivers (return_money) -> conflito na sintese.
--      -> Agora cada saida tem UMA unica origem (registrador).
--   4) Display em logica ATIVA-ALTA -> na placa os displays sao ATIVO-BAIXO.
--      -> Agora a tabela de 7 segmentos usa 0 = aceso, 1 = apagado.
--
-- MAPEAMENTO DE HARDWARE (ajuste os pinos no Pin Planner / .qsf do Quartus):
--   CLOCK_50 : clock de 50 MHz da placa
--   KEY (ATIVO-BAIXO, 0 = pressionado):
--     KEY(0) = confirmar salgado escolhido
--     KEY(1) = inserir a moeda selecionada
--     KEY(2) = desistir (devolve todas as moedas)
--     KEY(3) = reset geral
--   SW (1 = cima, 0 = baixo):
--     SW(2..0) = tipo do salgado (one-hot NAO; usar codigo abaixo)
--       "001" Batata frita grande   R$ 2,50
--       "010" Batata frita media    R$ 1,50
--       "011" Batata frita pequena  R$ 0,75
--       "100" Tortilha grande       R$ 3,50
--       "101" Tortilha pequena      R$ 2,00
--     SW(9..8) = moeda inserida (chaves do final da placa)
--       "01" = R$ 0,25 | "10" = R$ 0,50 | "11" = R$ 1,00 | "00" = invalida
--   HEX3 HEX2 = parte em REAIS    (dezena, unidade)
--   HEX1 HEX0 = parte em CENTAVOS (dezena, unidade)   -> mostra "XX.XX"
--   LEDG(0) = Libera_salgado
--   LEDG(1) = Libera_troco
--   LEDR(0) = Libera_todas_as_moedas (devolucao por desistencia)
--   LEDR(9) = Sem_estoque (pisca como aviso)
--------------------------------------------------------------------------------
library IEEE;
use IEEE.STD_LOGIC_1164.ALL;
use IEEE.NUMERIC_STD.ALL;

entity Vendingmachine is
    port (
        CLOCK_50 : in  STD_LOGIC;                       -- clock de 50 MHz
        KEY      : in  STD_LOGIC_VECTOR(3 downto 0);    -- botoes (ativo-baixo)
        SW       : in  STD_LOGIC_VECTOR(9 downto 0);    -- chaves

        HEX0     : out STD_LOGIC_VECTOR(6 downto 0);    -- centavos (unidade)
        HEX1     : out STD_LOGIC_VECTOR(6 downto 0);    -- centavos (dezena)
        HEX2     : out STD_LOGIC_VECTOR(6 downto 0);    -- reais (unidade)
        HEX3     : out STD_LOGIC_VECTOR(6 downto 0);    -- reais (dezena)

        LEDG     : out STD_LOGIC_VECTOR(7 downto 0);    -- LEDs verdes
        LEDR     : out STD_LOGIC_VECTOR(9 downto 0)     -- LEDs vermelhos
    );
end Vendingmachine;

architecture Behavioral of Vendingmachine is

    ----------------------------------------------------------------------------
    -- Estados da maquina (Unidade de Controle)
    ----------------------------------------------------------------------------
    type state_type is (
        IDLE,           -- pronto: escolhe o salgado
        CHECK_STOCK,    -- verifica se tem estoque
        ACCEPT_MONEY,   -- recebe moedas / permite desistir
        DISPENSE_ITEM,  -- libera o salgado e baixa o estoque
        FINAL_STATE,    -- mostra/libera o troco
        NO_STOCK,       -- aviso: sem estoque (pisca LED)
        RETURN_MONEY    -- desistencia: devolve todas as moedas
    );
    signal state : state_type := IDLE;

    ----------------------------------------------------------------------------
    -- Precos em centavos (250 = R$ 2,50)
    ----------------------------------------------------------------------------
    constant PRECO_BATATA_GRANDE    : integer := 250;
    constant PRECO_BATATA_MEDIA     : integer := 150;
    constant PRECO_BATATA_PEQUENA   : integer :=  75;
    constant PRECO_TORTILHA_GRANDE  : integer := 350;
    constant PRECO_TORTILHA_PEQUENA : integer := 200;

    ----------------------------------------------------------------------------
    -- Logica auxiliar (datapath)
    ----------------------------------------------------------------------------
    signal total_inserido : integer range 0 to 9999 := 0;  -- somatorio em centavos
    signal preco          : integer range 0 to 9999 := 0;  -- preco do item escolhido
    signal troco          : integer range 0 to 9999 := 0;  -- troco calculado
    signal item_sel       : STD_LOGIC_VECTOR(2 downto 0) := "000";

    -- Estoque inicial: 3 unidades de cada salgado (indices 0..4)
    type stock_array is array (0 to 4) of integer range 0 to 15;
    signal estoque : stock_array := (others => 3);

    ----------------------------------------------------------------------------
    -- Saidas registradas (cada uma com UMA unica origem -> sem conflito)
    ----------------------------------------------------------------------------
    signal libera_salgado : STD_LOGIC := '0';
    signal libera_troco   : STD_LOGIC := '0';
    signal devolve_moedas : STD_LOGIC := '0';

    ----------------------------------------------------------------------------
    -- Temporizador: segura os estados de saida para o olho humano ver os LEDs
    -- 25.000.000 ciclos de 50 MHz ~= 0,5 segundo
    ----------------------------------------------------------------------------
    constant HOLD_TIME : integer := 25000000;
    signal t_cnt : integer range 0 to HOLD_TIME := 0;

    ----------------------------------------------------------------------------
    -- Debounce e deteccao de borda dos botoes
    ----------------------------------------------------------------------------
    signal tick_cnt  : integer range 0 to 249999 := 0;       -- ~5 ms
    signal slow_en   : STD_LOGIC := '0';                     -- pulso de amostragem
    signal key_meta  : STD_LOGIC_VECTOR(3 downto 0) := "1111";
    signal key_sync  : STD_LOGIC_VECTOR(3 downto 0) := "1111";
    signal key_last  : STD_LOGIC_VECTOR(3 downto 0) := "1111";
    signal key_press : STD_LOGIC_VECTOR(3 downto 0) := "0000"; -- pulso de 1 clique

    ----------------------------------------------------------------------------
    -- Pisca-pisca do aviso de "sem estoque"
    ----------------------------------------------------------------------------
    signal blink_cnt : integer range 0 to 6250000 := 0;
    signal blink     : STD_LOGIC := '0';

    ----------------------------------------------------------------------------
    -- Valor que vai para os displays (somatorio normalmente; troco no final)
    ----------------------------------------------------------------------------
    signal disp_value : integer range 0 to 9999 := 0;

    ----------------------------------------------------------------------------
    -- Funcao: preco do salgado a partir do codigo da chave
    ----------------------------------------------------------------------------
    function preco_do(codigo : STD_LOGIC_VECTOR(2 downto 0)) return integer is
    begin
        case codigo is
            when "001"  => return PRECO_BATATA_GRANDE;
            when "010"  => return PRECO_BATATA_MEDIA;
            when "011"  => return PRECO_BATATA_PEQUENA;
            when "100"  => return PRECO_TORTILHA_GRANDE;
            when "101"  => return PRECO_TORTILHA_PEQUENA;
            when others => return 0;   -- selecao invalida
        end case;
    end function;

    ----------------------------------------------------------------------------
    -- Funcao: indice do estoque (0..4) a partir do codigo da chave
    ----------------------------------------------------------------------------
    function indice_do(codigo : STD_LOGIC_VECTOR(2 downto 0)) return integer is
    begin
        case codigo is
            when "001"  => return 0;
            when "010"  => return 1;
            when "011"  => return 2;
            when "100"  => return 3;
            when "101"  => return 4;
            when others => return 0;
        end case;
    end function;

    ----------------------------------------------------------------------------
    -- Funcao: digito (0..9) para 7 segmentos
    -- Ordem dos bits: (6..0) = g f e d c b a
    ----------------------------------------------------------------------------
    function to_7seg(d : integer) return STD_LOGIC_VECTOR is
    begin
        case d is
            when 0      => return "1000000";
            when 1      => return "1111001";
            when 2      => return "0100100";
            when 3      => return "0110000";
            when 4      => return "0011001";
            when 5      => return "0010010";
            when 6      => return "0000010";
            when 7      => return "1111000";
            when 8      => return "0000000";
            when 9      => return "0010000";
            when others => return "1111111";  -- apagado
        end case;
    end function;

begin

    ----------------------------------------------------------------------------
    -- (1) Gera pulso de amostragem lenta (~5 ms) para o debounce
    ----------------------------------------------------------------------------
    process (CLOCK_50)
    begin
        if rising_edge(CLOCK_50) then
            if tick_cnt >= 249999 then
                tick_cnt <= 0;
                slow_en  <= '1';
            else
                tick_cnt <= tick_cnt + 1;
                slow_en  <= '0';
            end if;
        end if;
    end process;

    ----------------------------------------------------------------------------
    -- (2) Sincroniza os botoes e gera um PULSO de 1 ciclo a cada clique
    --     Como KEY e ativo-baixo, "clique" = borda de descida (1 -> 0)
    ----------------------------------------------------------------------------
    process (CLOCK_50)
    begin
        if rising_edge(CLOCK_50) then
            -- dois flip-flops contra metaestabilidade
            key_meta <= KEY;
            key_sync <= key_meta;

            key_press <= "0000";                  -- por padrao, sem clique
            if slow_en = '1' then
                -- press(i) = estava solto (1) e agora esta apertado (0)
                key_press <= key_last and (not key_sync);
                key_last  <= key_sync;
            end if;
        end if;
    end process;

    ----------------------------------------------------------------------------
    -- (3) Pisca-pisca (~4 Hz) usado no aviso de sem estoque
    ----------------------------------------------------------------------------
    process (CLOCK_50)
    begin
        if rising_edge(CLOCK_50) then
            if blink_cnt >= 6250000 then
                blink_cnt <= 0;
                blink     <= not blink;
            else
                blink_cnt <= blink_cnt + 1;
            end if;
        end if;
    end process;

    ----------------------------------------------------------------------------
    -- (4) MAQUINA DE ESTADOS + DATAPATH
    --     Reset por KEY(3) (apertado = 0).
    ----------------------------------------------------------------------------
    process (CLOCK_50)
    begin
        if rising_edge(CLOCK_50) then
            if key_sync(3) = '0' then
                ------------------------------------------------------------------
                -- RESET geral
                ------------------------------------------------------------------
                state          <= IDLE;
                total_inserido <= 0;
                preco          <= 0;
                troco          <= 0;
                t_cnt          <= 0;
                item_sel       <= "000";
                libera_salgado <= '0';
                libera_troco   <= '0';
                devolve_moedas <= '0';
                -- Obs.: o estoque NAO e recarregado no reset (esgota de verdade)
            else
                case state is

                    --------------------------------------------------------------
                    -- IDLE: pronto para uma nova venda
                    --------------------------------------------------------------
                    when IDLE =>
                        libera_salgado <= '0';
                        libera_troco   <= '0';
                        devolve_moedas <= '0';
                        total_inserido <= 0;
                        troco          <= 0;
                        t_cnt          <= 0;
                        -- le continuamente a chave para saber o preco/item
                        preco    <= preco_do(SW(2 downto 0));
                        item_sel <= SW(2 downto 0);
                        -- so avanca quando o cliente CONFIRMA (KEY0)
                        if key_press(0) = '1' then
                            state <= CHECK_STOCK;
                        end if;

                    --------------------------------------------------------------
                    -- CHECK_STOCK: tem o salgado escolhido?
                    --------------------------------------------------------------
                    when CHECK_STOCK =>
                        t_cnt <= 0;
                        if preco = 0 then
                            state <= IDLE;                       -- selecao invalida
                        elsif estoque(indice_do(item_sel)) > 0 then
                            state <= ACCEPT_MONEY;               -- pode pagar
                        else
                            state <= NO_STOCK;                   -- emite aviso
                        end if;

                    --------------------------------------------------------------
                    -- ACCEPT_MONEY: recebe moedas ou aceita a desistencia
                    --------------------------------------------------------------
                    when ACCEPT_MONEY =>
                        t_cnt <= 0;
                        if key_press(2) = '1' then               -- desistir
                            state <= RETURN_MONEY;
                        elsif total_inserido >= preco then       -- pagou o bastante
                            state <= DISPENSE_ITEM;
                        elsif key_press(1) = '1' then            -- inseriu moeda
                            case SW(9 downto 8) is
                                when "01"   => total_inserido <= total_inserido + 25;
                                when "10"   => total_inserido <= total_inserido + 50;
                                when "11"   => total_inserido <= total_inserido + 100;
                                when others => null;  -- moeda invalida: nao soma
                            end case;
                        end if;

                    --------------------------------------------------------------
                    -- DISPENSE_ITEM: libera o salgado e baixa o estoque
                    --------------------------------------------------------------
                    when DISPENSE_ITEM =>
                        libera_salgado <= '1';
                        if t_cnt = 0 then
                            -- baixa o estoque e calcula o troco apenas uma vez
                            estoque(indice_do(item_sel)) <=
                                estoque(indice_do(item_sel)) - 1;
                            troco <= total_inserido - preco;
                        end if;
                        if t_cnt < HOLD_TIME then
                            t_cnt <= t_cnt + 1;
                        else
                            t_cnt <= 0;
                            state <= FINAL_STATE;
                        end if;

                    --------------------------------------------------------------
                    -- FINAL_STATE: mostra e libera o troco
                    --------------------------------------------------------------
                    when FINAL_STATE =>
                        libera_salgado <= '0';
                        libera_troco   <= '1';
                        if t_cnt < HOLD_TIME then
                            t_cnt <= t_cnt + 1;
                        else
                            t_cnt        <= 0;
                            libera_troco <= '0';
                            state        <= IDLE;
                        end if;

                    --------------------------------------------------------------
                    -- NO_STOCK: aviso de sem estoque (LED pisca) e volta
                    --------------------------------------------------------------
                    when NO_STOCK =>
                        if t_cnt < HOLD_TIME then
                            t_cnt <= t_cnt + 1;
                        else
                            t_cnt <= 0;
                            state <= IDLE;
                        end if;

                    --------------------------------------------------------------
                    -- RETURN_MONEY: desistencia -> devolve todas as moedas
                    --------------------------------------------------------------
                    when RETURN_MONEY =>
                        devolve_moedas <= '1';
                        if t_cnt < HOLD_TIME then
                            t_cnt <= t_cnt + 1;
                        else
                            t_cnt          <= 0;
                            devolve_moedas <= '0';
                            total_inserido <= 0;
                            state          <= IDLE;
                        end if;

                    when others =>
                        state <= IDLE;

                end case;
            end if;
        end if;
    end process;

    ----------------------------------------------------------------------------
    -- (5) Escolhe o que mostrar: troco no estado final, senao o somatorio
    ----------------------------------------------------------------------------
    disp_value <= troco when (state = FINAL_STATE) else total_inserido;

    ----------------------------------------------------------------------------
    -- (6) Converte o valor (centavos) para os 4 digitos dos displays
    --     HEX3 HEX2 = reais   |   HEX1 HEX0 = centavos
    ----------------------------------------------------------------------------
    process (disp_value)
        variable reais, cent : integer range 0 to 99;
    begin
        reais := (disp_value / 100) mod 100;  -- parte inteira (R$)
        cent  := disp_value mod 100;          -- parte de centavos
        HEX3 <= to_7seg(reais / 10);
        HEX2 <= to_7seg(reais mod 10);
        HEX1 <= to_7seg(cent / 10);
        HEX0 <= to_7seg(cent mod 10);
    end process;

    ----------------------------------------------------------------------------
    -- (7) LEDs (cada saida vem de uma unica origem registrada)
    ----------------------------------------------------------------------------
    LEDG(0)          <= libera_salgado;                   -- Libera_salgado
    LEDG(1)          <= libera_troco;                     -- Libera_troco
    LEDG(7 downto 2) <= (others => '0');

    LEDR(0)          <= devolve_moedas;                   -- Libera_todas_as_moedas
    LEDR(8 downto 1) <= (others => '0');
    LEDR(9)          <= blink when (state = NO_STOCK) else '0';  -- Sem_estoque

end Behavioral;
