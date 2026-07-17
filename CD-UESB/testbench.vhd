--------------------------------------------------------------------------------
-- testbench.vhd
-- Testbench para Vendingmachine.vhd (simulacao no ModelSim via Quartus)
--
-- IMPORTANTE sobre timing:
--   O circuito amostra botoes a cada ~5 ms (G_DEBOUNCE_MAX ciclos de clock).
--   O clique simulado precisa durar MAIS que isso, senao key_press nunca
--   dispara e state/total_inserido ficam parados em IDLE.
--
-- IMPORTANTE sobre troco (cenario 2):
--   Quando total_inserido >= preco, a FSM vai para DISPENSE no proximo ciclo.
--   Se inserir 2x R$1,00 num item de R$2,00, dispara com valor EXATO (sem
--   chance de inserir mais). Para testar TROCO, a ultima moeda deve fazer
--   total PASSAR de preco: ex. R$1 + R$0,50 + R$1 = R$2,50.
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

    constant CLK_PERIOD : time := 20 ns;  -- 50 MHz (igual a placa)

    -- Clique deve durar mais que o periodo de debounce (~5 ms @ 50 MHz)
    constant KEY_HOLD   : time := 12 ms;
    constant KEY_GAP    : time := 8 ms;

    signal CLOCK_50 : STD_LOGIC := '0';
    signal KEY      : STD_LOGIC_VECTOR(3 downto 0) := (others => '1');
    signal SW       : STD_LOGIC_VECTOR(9 downto 0) := (others => '0');
    signal HEX0, HEX1, HEX2, HEX3 : STD_LOGIC_VECTOR(6 downto 0);
    signal LEDG     : STD_LOGIC_VECTOR(7 downto 0);
    signal LEDR     : STD_LOGIC_VECTOR(9 downto 0);

    -- Simula apertar KEY(idx): segura ativo-baixo por KEY_HOLD
    procedure press(signal k : out STD_LOGIC_VECTOR; idx : integer) is
    begin
        k(idx) <= '0';
        wait for KEY_HOLD;
        k(idx) <= '1';
        wait for KEY_GAP;
    end procedure;

begin

    CLOCK_50 <= not CLOCK_50 after CLK_PERIOD / 2;

    uut : entity work.Vendingmachine
        generic map (
            G_HOLD_TIME    => 25000,   -- ~0,5 ms @ 50 MHz (rapido p/ simulacao)
            G_DEBOUNCE_MAX => 249999   -- ~5 ms @ 50 MHz (igual placa)
        )
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

        press(KEY, 3);  -- reset
        wait for 5 ms;

        ------------------------------------------------------------------
        -- Cenario 1: Batata media R$ 1,50 (codigo "010")
        -- Paga com 3x R$ 0,50 = R$ 1,50 exato
        ------------------------------------------------------------------
        report "Cenario 1: Batata media, pagamento exato";
        SW(2 downto 0) <= "010";
        SW(9 downto 8) <= "00";
        press(KEY, 0);  -- confirmar salgado

        SW(9 downto 8) <= "10";  -- R$ 0,50
        press(KEY, 1);
        press(KEY, 1);
        press(KEY, 1);

        -- Verifica durante a transacao (LEDs acumulam ate o IDLE)
        wait for 2 ms;
        assert LEDG(0) = '1'
            report "ERRO: LEDG0 deveria acender em ACCEPT_MONEY/DISPENSE"
            severity warning;

        wait for 5 ms;

        ------------------------------------------------------------------
        -- Cenario 2: Tortilha pequena R$ 2,00 (codigo "101")
        -- Paga R$ 2,50 -> troco R$ 0,50
        -- ORDEM: R$1 + R$0,50 + R$1 (NAO 2x R$1 seguidos!)
        ------------------------------------------------------------------
        report "Cenario 2: Tortilha pequena com troco";
        SW(2 downto 0) <= "101";
        press(KEY, 0);

        SW(9 downto 8) <= "11";  -- R$ 1,00  -> total 100
        press(KEY, 1);
        SW(9 downto 8) <= "10";  -- R$ 0,50  -> total 150
        press(KEY, 1);
        SW(9 downto 8) <= "11";  -- R$ 1,00  -> total 250 (passa de 200)
        press(KEY, 1);

        wait for 2 ms;
        assert LEDG(1) = '1'
            report "ERRO: LEDG1 deveria acender em DISPENSE/FINAL"
            severity warning;
        assert LEDG(3 downto 0) = "1111"
            report "ERRO: todos LEDG0..3 deveriam acender no FINAL_STATE"
            severity warning;

        wait for 5 ms;

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

        wait for 2 ms;
        assert LEDG(2 downto 0) = "111"
            report "ERRO: 3 LEDs verdes deveriam acender em RETURN_MONEY"
            severity warning;
        assert LEDR = "1111111111"
            report "ERRO: todos LEDR deveriam acender na devolucao"
            severity warning;

        wait for 5 ms;

        report "=== Simulacao concluida ===";
        wait;
    end process;

end sim;
