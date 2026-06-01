--------------------------------------------------------------------------------
-- fsm_control.vhd
-- Unidade de controle (FSM de Moore) da máquina de vender salgados.
-- Fluxo: Esperar Seleção -> Checar Estoque -> Aguardar Moedas ->
--        Liberar Produto -> Calcular Troco -> Liberar Troco
--        (ramo alternativo: Devolver Moedas em caso de desistência)
--------------------------------------------------------------------------------
library ieee;
    use ieee.std_logic_1164.all;
    use ieee.numeric_std.all;

entity fsm_control is
    generic (
        G_DISPENSE_CYCLES : positive := 5;  -- duração dos pulsos de liberação
        G_BLINK_DIV       : positive := 5   -- meio-período do LED sem estoque
    );
    port (
        clk             : in  std_logic;
        rst_n           : in  std_logic;

        -- Eventos e status
        prod_valid      : in  std_logic;
        stock_available : in  std_logic;
        payment_ok      : in  std_logic;
        change_gt_zero  : in  std_logic;

        pulse_cancel    : in  std_logic;
        pulse_coin25    : in  std_logic;
        pulse_coin50    : in  std_logic;
        pulse_coin1     : in  std_logic;

        -- Controles para o datapath
        latch_prod      : out std_logic;
        clr_inserted    : out std_logic;
        en_add_coin     : out std_logic;
        coin_value      : out std_logic_vector(13 downto 0);
        dec_stock       : out std_logic;

        -- Saídas para LEDs
        libera_salgado  : out std_logic;
        libera_troco    : out std_logic;
        devolve_moedas  : out std_logic;
        sem_estoque     : out std_logic   -- pisca quando sem estoque
    );
end entity fsm_control;

architecture rtl of fsm_control is

    -- Estados da máquina
    type t_state is (
        ST_WAIT_SELECTION,   -- Esperar seleção do produto (SW one-hot)
        ST_CHECK_STOCK,      -- Verificar estoque do item
        ST_WAIT_COINS,       -- Aguardar moedas até pagamento ou cancelamento
        ST_DISPENSE_PRODUCT, -- Liberar salgado
        ST_CALC_CHANGE,      -- Calcular troco (combinatório no datapath)
        ST_DISPENSE_CHANGE,  -- Liberar troco
        ST_RETURN_COINS,     -- Devolver todas as moedas (desistência)
        ST_NO_STOCK          -- Sem estoque: sinalizar e voltar
    );
    signal state, next_state : t_state;

    signal timer       : unsigned(7 downto 0);
    signal blink_cnt   : unsigned(7 downto 0);
    signal blink_led   : std_logic;

    -- Valores das moedas em centavos
    constant C_COIN_25  : std_logic_vector(13 downto 0) := std_logic_vector(to_unsigned(25, 14));
    constant C_COIN_50  : std_logic_vector(13 downto 0) := std_logic_vector(to_unsigned(50, 14));
    constant C_COIN_100 : std_logic_vector(13 downto 0) := std_logic_vector(to_unsigned(100, 14));

    -- Sinais de controle (Moore: derivados do estado)
    signal ctrl_latch_prod   : std_logic;
    signal ctrl_clr_inserted : std_logic;
    signal ctrl_en_add_coin  : std_logic;
    signal ctrl_coin_value   : std_logic_vector(13 downto 0);
    signal ctrl_dec_stock    : std_logic;
    signal ctrl_libera_salg  : std_logic;
    signal ctrl_libera_troco : std_logic;
    signal ctrl_devolve      : std_logic;

    signal any_coin_pulse    : std_logic;

begin

    any_coin_pulse <= pulse_coin25 or pulse_coin50 or pulse_coin1;

    -- =========================================================================
    -- Registrador de estado (clock FSM ~10 Hz, reset assíncrono ativo em '0')
    -- =========================================================================
    process (clk, rst_n)
    begin
        if rst_n = '0' then
            state <= ST_WAIT_SELECTION;
        elsif rising_edge(clk) then
            state <= next_state;
        end if;
    end process;

    -- =========================================================================
    -- Timer para estados temporizados (liberação / sem estoque)
    -- =========================================================================
    process (clk, rst_n)
    begin
        if rst_n = '0' then
            timer <= (others => '0');
        elsif rising_edge(clk) then
            if state /= next_state then
                timer <= (others => '0');
            else
                timer <= timer + 1;
            end if;
        end if;
    end process;

    -- =========================================================================
    -- Pisca LED sem estoque (LEDR[9])
    -- =========================================================================
    process (clk, rst_n)
    begin
        if rst_n = '0' then
            blink_cnt <= (others => '0');
            blink_led <= '0';
        elsif rising_edge(clk) then
            if state = ST_NO_STOCK then
                if blink_cnt = G_BLINK_DIV - 1 then
                    blink_cnt <= (others => '0');
                    blink_led <= not blink_led;
                else
                    blink_cnt <= blink_cnt + 1;
                end if;
            else
                blink_cnt <= (others => '0');
                blink_led <= '0';
            end if;
        end if;
    end process;

    sem_estoque <= blink_led when state = ST_NO_STOCK else '0';

    -- =========================================================================
    -- Lógica de próximo estado
    -- =========================================================================
    process (state, prod_valid, stock_available, payment_ok, change_gt_zero,
             pulse_cancel, any_coin_pulse, timer)
    begin
        next_state <= state;

        case state is

            when ST_WAIT_SELECTION =>
                -- Aguarda SW one-hot válida
                if prod_valid = '1' then
                    next_state <= ST_CHECK_STOCK;
                end if;

            when ST_CHECK_STOCK =>
                if stock_available = '1' then
                    next_state <= ST_WAIT_COINS;
                else
                    next_state <= ST_NO_STOCK;
                end if;

            when ST_WAIT_COINS =>
                -- Cancelamento tem prioridade sobre pagamento completo
                if pulse_cancel = '1' then
                    next_state <= ST_RETURN_COINS;
                elsif payment_ok = '1' then
                    next_state <= ST_DISPENSE_PRODUCT;
                end if;

            when ST_DISPENSE_PRODUCT =>
                if timer = G_DISPENSE_CYCLES - 1 then
                    next_state <= ST_CALC_CHANGE;
                end if;

            when ST_CALC_CHANGE =>
                -- Troco já calculado no datapath; decisão imediata
                if change_gt_zero = '1' then
                    next_state <= ST_DISPENSE_CHANGE;
                else
                    next_state <= ST_WAIT_SELECTION;
                end if;

            when ST_DISPENSE_CHANGE =>
                if timer = G_DISPENSE_CYCLES - 1 then
                    next_state <= ST_WAIT_SELECTION;
                end if;

            when ST_RETURN_COINS =>
                if timer = G_DISPENSE_CYCLES - 1 then
                    next_state <= ST_WAIT_SELECTION;
                end if;

            when ST_NO_STOCK =>
                -- Permanece piscando; ao soltar seleção inválida, volta
                if prod_valid = '0' then
                    next_state <= ST_WAIT_SELECTION;
                end if;

            when others =>
                next_state <= ST_WAIT_SELECTION;

        end case;
    end process;

    -- =========================================================================
    -- Saídas de controle (Moore por estado)
    -- =========================================================================
    process (state, pulse_coin25, pulse_coin50, pulse_coin1, timer)
    begin
        -- Padrão inativo
        ctrl_latch_prod   <= '0';
        ctrl_clr_inserted <= '0';
        ctrl_en_add_coin  <= '0';
        ctrl_coin_value   <= (others => '0');
        ctrl_dec_stock    <= '0';
        ctrl_libera_salg  <= '0';
        ctrl_libera_troco <= '0';
        ctrl_devolve      <= '0';

        case state is

            when ST_WAIT_SELECTION =>
                ctrl_clr_inserted <= '1';  -- display em 00.00

            when ST_CHECK_STOCK =>
                ctrl_latch_prod <= '1';    -- trava produto e preço

            when ST_WAIT_COINS =>
                -- Aceita moedas válidas; ignora moedas inválidas (sem pulso)
                if pulse_coin25 = '1' then
                    ctrl_en_add_coin <= '1';
                    ctrl_coin_value  <= C_COIN_25;
                elsif pulse_coin50 = '1' then
                    ctrl_en_add_coin <= '1';
                    ctrl_coin_value  <= C_COIN_50;
                elsif pulse_coin1 = '1' then
                    ctrl_en_add_coin <= '1';
                    ctrl_coin_value  <= C_COIN_100;
                end if;

            when ST_DISPENSE_PRODUCT =>
                ctrl_libera_salg <= '1';
                if timer = 0 then
                    ctrl_dec_stock <= '1';  -- decrementa estoque no 1º ciclo
                end if;

            when ST_CALC_CHANGE =>
                null;  -- troco é combinacional

            when ST_DISPENSE_CHANGE =>
                ctrl_libera_troco <= '1';
                if timer = G_DISPENSE_CYCLES - 1 then
                    ctrl_clr_inserted <= '1';
                end if;

            when ST_RETURN_COINS =>
                ctrl_devolve <= '1';
                if timer = G_DISPENSE_CYCLES - 1 then
                    ctrl_clr_inserted <= '1';
                end if;

            when ST_NO_STOCK =>
                null;

            when others =>
                null;

        end case;
    end process;

  latch_prod   <= ctrl_latch_prod;
  clr_inserted <= ctrl_clr_inserted;
  en_add_coin  <= ctrl_en_add_coin;
  coin_value   <= ctrl_coin_value;
  dec_stock    <= ctrl_dec_stock;

  libera_salgado <= ctrl_libera_salg;
  libera_troco   <= ctrl_libera_troco;
  devolve_moedas <= ctrl_devolve;

end architecture rtl;
