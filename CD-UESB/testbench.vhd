--------------------------------------------------------------------------------
-- testbench.vhd
-- Testbench para Vendingmachine.vhd (simulacao no ModelSim via Quartus)
--
-- Cenarios cobertos:
--   1) Compra batata media (R$ 1,50) com pagamento exato
--   2) Tortilha pequena (R$ 2,00) com troco
--   3) Desistencia (devolve moedas)
--------------------------------------------------------------------------------
library IEEE;
use IEEE.STD_LOGIC_1164.ALL;

entity testbench is
end testbench;

architecture sim of testbench is

    constant CLK_PERIOD : time := 20 ns;  -- 50 MHz

    signal CLOCK_50 : STD_LOGIC := '0';
    signal KEY      : STD_LOGIC_VECTOR(3 downto 0) := (others => '1');
    signal SW       : STD_LOGIC_VECTOR(9 downto 0) := (others => '0');
    signal HEX0, HEX1, HEX2, HEX3 : STD_LOGIC_VECTOR(6 downto 0);
    signal LEDG     : STD_LOGIC_VECTOR(7 downto 0);
    signal LEDR     : STD_LOGIC_VECTOR(9 downto 0);

    -- Aperta KEY(i) por um curto tempo (ativo-baixo)
    procedure press(signal k : out STD_LOGIC_VECTOR; idx : integer) is
    begin
        k(idx) <= '0';
        wait for 200 us;
        k(idx) <= '1';
        wait for 500 us;
    end procedure;

begin

    CLOCK_50 <= not CLOCK_50 after CLK_PERIOD / 2;

    uut : entity work.Vendingmachine
        port map (
            CLOCK_50 => CLOCK_50,
            KEY      => KEY,
            SW       => SW,
            HEX0     => HEX0,
            HEX1     => HEX1,
            HEX2     => HEX2,
            HEX3     => HEX3,
            LEDG     => LEDG,
            LEDR     => LEDR
        );

    process
    begin
        report "=== Inicio da simulacao ===";

        -- Reset (KEY3)
        press(KEY, 3);
        wait for 1 ms;

        ------------------------------------------------------------------
        -- Cenario 1: Batata media R$ 1,50 (codigo "010")
        -- Paga com 3x R$ 0,50
        ------------------------------------------------------------------
        report "Cenario 1: Batata media, pagamento exato";
        SW(2 downto 0) <= "010";
        SW(9 downto 8) <= "00";
        press(KEY, 0);  -- confirmar salgado

        SW(9 downto 8) <= "10";  -- R$ 0,50
        press(KEY, 1);
        press(KEY, 1);
        press(KEY, 1);

        wait for 2 ms;
        assert LEDG(0) = '1' report "ERRO: LEDG0 deveria acender" severity warning;

        wait for 60 ms;

        ------------------------------------------------------------------
        -- Cenario 2: Tortilha pequena R$ 2,00 (codigo "101")
        -- Paga R$ 2,50 -> troco R$ 0,50
        ------------------------------------------------------------------
        report "Cenario 2: Tortilha pequena com troco";
        SW(2 downto 0) <= "101";

        press(KEY, 0);

        SW(9 downto 8) <= "11";  -- R$ 1,00
        press(KEY, 1);
        press(KEY, 1);
        SW(9 downto 8) <= "10";  -- R$ 0,50
        press(KEY, 1);

        wait for 70 ms;
        assert LEDG(1) = '1' report "ERRO: LEDG1 deveria acender (troco)" severity warning;

        wait for 60 ms;

        ------------------------------------------------------------------
        -- Cenario 3: Desistencia
        ------------------------------------------------------------------
        report "Cenario 3: Desistencia";
        SW(2 downto 0) <= "011";  -- batata pequena R$ 0,75
        press(KEY, 0);

        SW(9 downto 8) <= "01";  -- R$ 0,25
        press(KEY, 1);
        press(KEY, 1);

        press(KEY, 2);  -- desistir

        wait for 60 ms;
        assert LEDR(0) = '1' report "ERRO: LEDR0 deveria acender (devolucao)" severity warning;

        report "=== Simulacao concluida ===";
        wait;
    end process;

end sim;
