--------------------------------------------------------------------------------
-- testbench.vhd
-- Testbench básico para simulação no ModelSim/Questa.
-- Acelera o divisor de clock da FSM para tornar a simulação viável.
--------------------------------------------------------------------------------
library ieee;
    use ieee.std_logic_1164.all;
    use ieee.numeric_std.all;

entity testbench is
end entity testbench;

architecture sim of testbench is

    -- Período do clock 50 MHz (20 ns)
    constant C_CLK_PERIOD : time := 20 ns;

    signal CLOCK_50 : std_logic := '0';
    signal KEY      : std_logic_vector(3 downto 0) := (others => '1');
    signal SW       : std_logic_vector(17 downto 0) := (others => '0');

    signal HEX0, HEX1, HEX2, HEX3 : std_logic_vector(6 downto 0);
    signal LEDG : std_logic_vector(7 downto 0);
    signal LEDR : std_logic_vector(9 downto 0);

    -- Componente top com generic reduzido (instanciado via entity work)
    component vending_machine_top is
        port (
            CLOCK_50 : in  std_logic;
            KEY      : in  std_logic_vector(3 downto 0);
            SW       : in  std_logic_vector(17 downto 0);
            HEX0     : out std_logic_vector(6 downto 0);
            HEX1     : out std_logic_vector(6 downto 0);
            HEX2     : out std_logic_vector(6 downto 0);
            HEX3     : out std_logic_vector(6 downto 0);
            LEDG     : out std_logic_vector(7 downto 0);
            LEDR     : out std_logic_vector(9 downto 0)
        );
    end component;

    -- Para teste rápido, recompilamos interface_io com divisor menor via
    -- entidade auxiliar: aqui usamos o top padrão e aceitamos simulação longa,
    -- OU instanciamos submódulos. Para simplicidade, instanciamos submódulos
    -- diretamente com divisores reduzidos.

    signal rst_n           : std_logic := '0';
    signal clk_fsm         : std_logic;
    signal pulse_cancel    : std_logic;
    signal pulse_coin25    : std_logic;
    signal pulse_coin50    : std_logic;
    signal pulse_coin1     : std_logic;
    signal prod_valid      : std_logic;
    signal prod_index      : std_logic_vector(2 downto 0);
    signal value_cents     : std_logic_vector(13 downto 0);
    signal change_cents    : std_logic_vector(13 downto 0);
    signal stock_available : std_logic;
    signal payment_ok      : std_logic;
    signal change_gt_zero  : std_logic;
    signal latch_prod      : std_logic;
    signal clr_inserted    : std_logic;
    signal en_add_coin     : std_logic;
    signal coin_value      : std_logic_vector(13 downto 0);
    signal dec_stock       : std_logic;
    signal libera_salgado  : std_logic;
    signal libera_troco    : std_logic;
    signal devolve_moedas  : std_logic;
    signal sem_estoque     : std_logic;

    -- Procedimento: pressiona KEY active-low por 3 ciclos de 50 MHz
    procedure press_key(signal key_line : out std_logic) is
    begin
        key_line <= '0';
        wait for 3 * C_CLK_PERIOD;
        key_line <= '1';
        wait for 5 * C_CLK_PERIOD;
    end procedure;

begin

    -- Clock 50 MHz
    CLOCK_50 <= not CLOCK_50 after C_CLK_PERIOD / 2;

    -- Reset via SW(17)
    SW(17) <= not rst_n;

    -- -------------------------------------------------------------------------
    -- DUT desmembrado com divisores acelerados (simulação rápida)
    -- -------------------------------------------------------------------------
    u_io : entity work.interface_io
        generic map (
            G_CLK_DIV_FSM  => 1000,    -- ~50 kHz -> visível na simulação
            G_DEBOUNCE_MAX => 50
        )
        port map (
            clk_50mhz    => CLOCK_50,
            rst_n        => rst_n,
            key          => KEY,
            sw           => SW(4 downto 0),
            value_cents  => value_cents,
            clk_fsm      => clk_fsm,
            pulse_cancel => pulse_cancel,
            pulse_coin25 => pulse_coin25,
            pulse_coin50 => pulse_coin50,
            pulse_coin1  => pulse_coin1,
            prod_valid   => prod_valid,
            prod_index   => prod_index,
            hex0         => HEX0,
            hex1         => HEX1,
            hex2         => HEX2,
            hex3         => HEX3
        );

    u_datapath : entity work.datapath
        generic map (G_INIT_STOCK => 3)
        port map (
            clk             => clk_fsm,
            rst_n           => rst_n,
            prod_index      => prod_index,
            latch_prod      => latch_prod,
            clr_inserted    => clr_inserted,
            en_add_coin     => en_add_coin,
            coin_value      => coin_value,
            dec_stock       => dec_stock,
            stock_available => stock_available,
            payment_ok      => payment_ok,
            change_gt_zero  => change_gt_zero,
            value_cents     => value_cents,
            change_cents    => change_cents
        );

    u_fsm : entity work.fsm_control
        generic map (
            G_DISPENSE_CYCLES => 3,
            G_BLINK_DIV       => 2
        )
        port map (
            clk             => clk_fsm,
            rst_n           => rst_n,
            prod_valid      => prod_valid,
            stock_available => stock_available,
            payment_ok      => payment_ok,
            change_gt_zero  => change_gt_zero,
            pulse_cancel    => pulse_cancel,
            pulse_coin25    => pulse_coin25,
            pulse_coin50    => pulse_coin50,
            pulse_coin1     => pulse_coin1,
            latch_prod      => latch_prod,
            clr_inserted    => clr_inserted,
            en_add_coin     => en_add_coin,
            coin_value      => coin_value,
            dec_stock       => dec_stock,
            libera_salgado  => libera_salgado,
            libera_troco    => libera_troco,
            devolve_moedas  => devolve_moedas,
            sem_estoque     => sem_estoque
        );

    LEDG(0) <= libera_salgado;
    LEDG(1) <= libera_troco;
    LEDR(0) <= devolve_moedas;
    LEDR(9) <= sem_estoque;

    -- -------------------------------------------------------------------------
    -- Estímulos de teste
    -- -------------------------------------------------------------------------
    process
    begin
        report "=== Início da simulação: Máquina de Salgados ===";

        -- Reset inicial
        rst_n <= '0';
        wait for 200 ns;
        rst_n <= '1';
        wait for 500 ns;

        -- ---------------------------------------------------------------------
        -- Cenário 1: Compra Batata Grande (SW[0]) R$ 2,50
        -- Inserir: 1x R$1,00 + 3x R$0,50 = R$ 2,50 (troco zero)
        -- ---------------------------------------------------------------------
        report "Cenário 1: Batata Grande R$ 2,50";
        SW <= (others => '0');
        SW(0) <= '1';  -- seleção one-hot
        wait for 50 us;  -- aguarda FSM processar seleção e estoque

        press_key(KEY(3));  -- R$ 1,00
        wait for 30 us;
        press_key(KEY(2));  -- R$ 0,50
        wait for 30 us;
        press_key(KEY(2));  -- R$ 0,50
        wait for 30 us;
        press_key(KEY(2));  -- R$ 0,50
        wait for 100 us;  -- libera produto

        assert libera_salgado = '1'
            report "ERRO: LEDG[0] deveria acender ao liberar salgado"
            severity warning;

        wait for 200 us;

        -- ---------------------------------------------------------------------
        -- Cenário 2: Tortilha pequena (SW[4]) R$ 2,00 com troco
        -- Inserir R$ 2,50 -> troco R$ 0,50
        -- ---------------------------------------------------------------------
        report "Cenário 2: Tortilha pequena R$ 2,00 com troco";
        SW <= (others => '0');
        SW(4) <= '1';
        wait for 50 us;

        press_key(KEY(2));  -- 0,50
        wait for 30 us;
        press_key(KEY(2));  -- 0,50
        wait for 30 us;
        press_key(KEY(2));  -- 0,50
        wait for 30 us;
        press_key(KEY(2));  -- 0,50
        wait for 30 us;
        press_key(KEY(2));  -- 0,50
        wait for 150 us;

        assert libera_troco = '1'
            report "ERRO: LEDG[1] deveria acender ao liberar troco"
            severity warning;

        wait for 200 us;

        -- ---------------------------------------------------------------------
        -- Cenário 3: Desistência (cancelamento)
        -- ---------------------------------------------------------------------
        report "Cenário 3: Desistência e devolução de moedas";
        SW <= (others => '0');
        SW(2) <= '1';  -- Batata pequena R$ 0,75
        wait for 50 us;

        press_key(KEY(1));  -- R$ 0,25
        wait for 30 us;
        press_key(KEY(1));  -- R$ 0,25
        wait for 30 us;
        press_key(KEY(0));  -- CANCELAR
        wait for 100 us;

        assert devolve_moedas = '1'
            report "ERRO: LEDR[0] deveria acender na devolução"
            severity warning;

        wait for 200 us;

        report "=== Simulação concluída ===";
        wait;
    end process;

end architecture sim;
