--------------------------------------------------------------------------------
-- vending_machine_top.vhd
-- Top-Level da Máquina de Vender Salgados para placa Altera DE1
-- (Cyclone II EP2C20F484C7N)
--
-- Mapeamento resumido:
--   CLOCK_50     : clock 50 MHz
--   KEY[0]       : cancelar / desistência (devolve moedas)
--   KEY[1..3]    : moedas R$ 0,25 / 0,50 / 1,00
--   SW[0..4]     : seleção one-hot do salgado
--   HEX0..HEX3   : valor inserido (formato posicional 00.00)
--   LEDG[0]      : libera salgado
--   LEDG[1]      : libera troco
--   LEDR[0]      : devolve todas as moedas
--   LEDR[9]      : sem estoque (piscando)
--------------------------------------------------------------------------------
library ieee;
    use ieee.std_logic_1164.all;

entity vending_machine_top is
    port (
        CLOCK_50 : in  std_logic;
        KEY      : in  std_logic_vector(3 downto 0);
        SW       : in  std_logic_vector(17 downto 0);

        HEX0 : out std_logic_vector(6 downto 0);
        HEX1 : out std_logic_vector(6 downto 0);
        HEX2 : out std_logic_vector(6 downto 0);
        HEX3 : out std_logic_vector(6 downto 0);

        LEDG : out std_logic_vector(7 downto 0);
        LEDR : out std_logic_vector(9 downto 0)
    );
end entity vending_machine_top;

architecture structural of vending_machine_top is

    -- Reset global: SW[17] = '1' aplica reset (KEY[0] é apenas cancelamento na FSM).
    signal rst_n      : std_logic;

    -- Clock FSM (~10 Hz) e sinais de interface
    signal clk_fsm        : std_logic;
    signal pulse_cancel   : std_logic;
    signal pulse_coin25   : std_logic;
    signal pulse_coin50   : std_logic;
    signal pulse_coin1    : std_logic;
    signal prod_valid     : std_logic;
    signal prod_index     : std_logic_vector(2 downto 0);

    -- Barramento datapath <-> FSM
    signal value_cents    : std_logic_vector(13 downto 0);
    signal change_cents   : std_logic_vector(13 downto 0);
    signal stock_available: std_logic;
    signal payment_ok     : std_logic;
    signal change_gt_zero : std_logic;

    signal latch_prod     : std_logic;
    signal clr_inserted   : std_logic;
    signal en_add_coin    : std_logic;
    signal coin_value     : std_logic_vector(13 downto 0);
    signal dec_stock      : std_logic;

    signal libera_salgado : std_logic;
    signal libera_troco   : std_logic;
    signal devolve_moedas : std_logic;
    signal sem_estoque    : std_logic;

begin

    -- SW[17] como reset manual (1 = reset). KEY[0] é apenas cancelamento na FSM.
    rst_n <= not SW(17);

    -- LEDs: demais bits apagados
    LEDG(0) <= libera_salgado;
    LEDG(1) <= libera_troco;
    LEDG(7 downto 2) <= (others => '0');

    LEDR(0) <= devolve_moedas;
    LEDR(8 downto 1) <= (others => '0');
    LEDR(9) <= sem_estoque;

    -- -------------------------------------------------------------------------
    -- Interface I/O (debounce, clock lento, displays)
    -- -------------------------------------------------------------------------
    u_io : entity work.interface_io
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

    -- -------------------------------------------------------------------------
    -- Datapath (valor inserido, estoque, troco)
    -- -------------------------------------------------------------------------
    u_datapath : entity work.datapath
        generic map (
            G_INIT_STOCK => 3
        )
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

    -- -------------------------------------------------------------------------
    -- FSM de controle
    -- -------------------------------------------------------------------------
    u_fsm : entity work.fsm_control
        generic map (
            G_DISPENSE_CYCLES => 5,
            G_BLINK_DIV       => 5
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

end architecture structural;
