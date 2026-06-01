--------------------------------------------------------------------------------
-- interface_io.vhd
-- Interface com o mundo físico da placa DE1:
--   - Divisor de clock (~10 Hz) para a FSM
--   - Sincronização, debounce e detecção de borda (KEY active-low)
--   - Decodificação one-hot das chaves de produto (SW[4:0])
--   - Conversão do valor monetário (centavos) para 4 dígitos BCD / 7 segmentos
--------------------------------------------------------------------------------
library ieee;
    use ieee.std_logic_1164.all;
    use ieee.numeric_std.all;

entity interface_io is
    generic (
        -- Meio-período do clk_fsm em ciclos de 50 MHz (2_500_000 => ~10 Hz)
        G_CLK_DIV_FSM : natural := 2_500_000;
        -- Debounce: ~20 ms @ 50 MHz
        G_DEBOUNCE_MAX : natural := 1_000_000
    );
    port (
        clk_50mhz   : in  std_logic;
        rst_n       : in  std_logic;

        -- Entradas físicas
        key         : in  std_logic_vector(3 downto 0);  -- active-low
        sw          : in  std_logic_vector(4 downto 0);    -- produtos SW[0..4]

        -- Valor a exibir (em centavos), vindo do datapath
        value_cents : in  std_logic_vector(13 downto 0);

        -- Clock lento para FSM e LEDs de status
        clk_fsm     : out std_logic;

        -- Pulsos de botão no domínio clk_fsm (1 ciclo, após debounce + borda)
        pulse_cancel : out std_logic;
        pulse_coin25 : out std_logic;
        pulse_coin50 : out std_logic;
        pulse_coin1  : out std_logic;

        -- Seleção de produto válida (exatamente uma SW ativa)
        prod_valid   : out std_logic;
        prod_index   : out std_logic_vector(2 downto 0);

        -- Displays 7 segmentos (active-low, catodo comum: 0 = acende)
        hex0, hex1, hex2, hex3 : out std_logic_vector(6 downto 0)
    );
end entity interface_io;

architecture rtl of interface_io is

    -- -------------------------------------------------------------------------
    -- Divisor de clock para FSM (~10 Hz, duty cycle 50%)
    -- -------------------------------------------------------------------------
    signal cnt_fsm   : unsigned(23 downto 0);
    signal clk_fsm_i : std_logic;

    -- -------------------------------------------------------------------------
    -- Debounce e detecção de borda descendente nos KEY (active-low)
    -- -------------------------------------------------------------------------
    type t_key_deb is record
        sync1, sync2 : std_logic;
        stable       : std_logic;
        counter      : unsigned(23 downto 0);
        last_edge    : std_logic;
    end record;

    type t_key_array is array (0 to 3) of t_key_deb;
    signal keys      : t_key_array;

    signal key_db    : std_logic_vector(3 downto 0);
    signal key_prev  : std_logic_vector(3 downto 0);
    signal key_fall  : std_logic_vector(3 downto 0);

    -- Requisições de pulso (domínio 50 MHz) e entrega no domínio clk_fsm
    signal req_cancel, req_coin25, req_coin50, req_coin1 : std_logic;
    signal pulse_cancel_i, pulse_coin25_i, pulse_coin50_i, pulse_coin1_i : std_logic;

    -- -------------------------------------------------------------------------
    -- BCD / 7 segmentos
    -- -------------------------------------------------------------------------
    signal d0, d1, d2, d3 : std_logic_vector(3 downto 0);  -- UDC, DDC, UR, DR
    signal val_u          : unsigned(13 downto 0);

    -- Tabela 7 segmentos DE1 (g f e d c b a), active-low
    function f_seg7(digit : std_logic_vector(3 downto 0)) return std_logic_vector is
    begin
        case digit is
            when "0000" => return "1000000";  -- 0
            when "0001" => return "1111001";  -- 1
            when "0010" => return "0100100";  -- 2
            when "0011" => return "0110000";  -- 3
            when "0100" => return "0011001";  -- 4
            when "0101" => return "1011010";  -- 5
            when "0110" => return "0010010";  -- 6
            when "0111" => return "1110000";  -- 7
            when "1000" => return "0000000";  -- 8
            when "1001" => return "0010000";  -- 9
            when others => return "1111111";  -- apagado
        end case;
    end function;

    -- Converte centavos (0..9999) em 4 dígitos BCD: DR UR DDC UDC
    procedure p_cents_to_bcd(
        cents : in  unsigned(13 downto 0);
        b3, b2, b1, b0 : out std_logic_vector(3 downto 0)
    ) is
        variable v      : integer;
        variable tens_r : integer;
        variable ones_r : integer;
        variable tens_c : integer;
        variable ones_c : integer;
    begin
        v := to_integer(cents);
        if v > 9999 then
            v := 9999;
        end if;
        tens_r := v / 1000;
        v      := v mod 1000;
        ones_r := v / 100;
        v      := v mod 100;
        tens_c := v / 10;
        ones_c := v mod 10;
        b3 := std_logic_vector(to_unsigned(tens_r, 4));
        b2 := std_logic_vector(to_unsigned(ones_r, 4));
        b1 := std_logic_vector(to_unsigned(tens_c, 4));
        b0 := std_logic_vector(to_unsigned(ones_c, 4));
    end procedure;

    -- Conta SW ativas e retorna índice se one-hot
    function f_decode_product(sw_in : std_logic_vector(4 downto 0))
        return std_logic_vector is
        variable cnt : integer := 0;
        variable idx : integer := 0;
    begin
        for i in 0 to 4 loop
            if sw_in(i) = '1' then
                cnt := cnt + 1;
                idx := i;
            end if;
        end loop;
        if cnt = 1 then
            return std_logic_vector(to_unsigned(idx, 3));
        else
            return (others => '1');  -- inválido
        end if;
    end function;

begin

    -- =========================================================================
    -- Divisor ~10 Hz (meio-período = G_CLK_DIV_FSM ciclos de 50 MHz)
    -- =========================================================================
    process (clk_50mhz, rst_n)
    begin
        if rst_n = '0' then
            cnt_fsm   <= (others => '0');
            clk_fsm_i <= '0';
        elsif rising_edge(clk_50mhz) then
            if cnt_fsm = G_CLK_DIV_FSM - 1 then
                cnt_fsm   <= (others => '0');
                clk_fsm_i <= not clk_fsm_i;
            else
                cnt_fsm <= cnt_fsm + 1;
            end if;
        end if;
    end process;

    clk_fsm <= clk_fsm_i;

    -- =========================================================================
    -- Debounce por botão (amostragem em 50 MHz)
    -- =========================================================================
    process (clk_50mhz, rst_n)
        variable v_db : std_logic;
    begin
        if rst_n = '0' then
            for i in 0 to 3 loop
                keys(i).sync1    <= '1';
                keys(i).sync2    <= '1';
                keys(i).stable   <= '1';
                keys(i).counter  <= (others => '0');
                keys(i).last_edge <= '0';
            end loop;
            key_db   <= (others => '1');
            key_prev <= (others => '1');
        elsif rising_edge(clk_50mhz) then
            for i in 0 to 3 loop
                -- Sincronizador (metastabilidade)
                keys(i).sync1 <= key(i);
                keys(i).sync2 <= keys(i).sync1;
                v_db := keys(i).sync2;

                if v_db /= keys(i).stable then
                    keys(i).counter <= (others => '0');
                elsif keys(i).counter < G_DEBOUNCE_MAX then
                    keys(i).counter <= keys(i).counter + 1;
                else
                    keys(i).stable <= v_db;
                end if;
            end loop;

            key_prev <= key_db;
            key_db(0) <= keys(0).stable;
            key_db(1) <= keys(1).stable;
            key_db(2) <= keys(2).stable;
            key_db(3) <= keys(3).stable;
        end if;
    end process;

    -- Borda de descida: estava alto (solto) e ficou baixo (pressionado)
    key_fall <= (not key_db) and key_prev;

    -- Captura borda de descida no domínio rápido (50 MHz)
    process (clk_50mhz, rst_n)
    begin
        if rst_n = '0' then
            req_cancel <= '0';
            req_coin25 <= '0';
            req_coin50 <= '0';
            req_coin1  <= '0';
        elsif rising_edge(clk_50mhz) then
            if key_fall(0) = '1' then req_cancel <= '1'; end if;
            if key_fall(1) = '1' then req_coin25 <= '1'; end if;
            if key_fall(2) = '1' then req_coin50 <= '1'; end if;
            if key_fall(3) = '1' then req_coin1  <= '1'; end if;
        end if;
    end process;

    -- Entrega 1 pulso por requisição no domínio clk_fsm (sincronizado)
    process (clk_fsm_i, rst_n)
    begin
        if rst_n = '0' then
            pulse_cancel_i <= '0';
            pulse_coin25_i <= '0';
            pulse_coin50_i <= '0';
            pulse_coin1_i  <= '0';
        elsif rising_edge(clk_fsm_i) then
            pulse_cancel_i <= '0';
            pulse_coin25_i <= '0';
            pulse_coin50_i <= '0';
            pulse_coin1_i  <= '0';
            if req_cancel = '1' then
                pulse_cancel_i <= '1';
                req_cancel     <= '0';
            end if;
            if req_coin25 = '1' then
                pulse_coin25_i <= '1';
                req_coin25     <= '0';
            end if;
            if req_coin50 = '1' then
                pulse_coin50_i <= '1';
                req_coin50     <= '0';
            end if;
            if req_coin1 = '1' then
                pulse_coin1_i  <= '1';
                req_coin1      <= '0';
            end if;
        end if;
    end process;

    pulse_cancel <= pulse_cancel_i;
    pulse_coin25 <= pulse_coin25_i;
    pulse_coin50 <= pulse_coin50_i;
    pulse_coin1  <= pulse_coin1_i;

    -- =========================================================================
    -- Decodificação one-hot do produto
    -- =========================================================================
    process (sw)
        variable cnt : integer;
    begin
        cnt := 0;
        for i in 0 to 4 loop
            if sw(i) = '1' then
                cnt := cnt + 1;
            end if;
        end loop;
        if cnt = 1 then
            prod_valid <= '1';
            prod_index <= f_decode_product(sw);
        else
            prod_valid <= '0';
            prod_index <= (others => '1');
        end if;
    end process;

    -- =========================================================================
    -- Valor monetário -> BCD -> 7 segmentos (formato posicional 00.00)
    -- HEX3=dezenas R$, HEX2=unidades R$, HEX1=dezenas cent, HEX0=unid cent
    -- =========================================================================
    val_u <= unsigned(value_cents);

    process (val_u)
    begin
        p_cents_to_bcd(val_u, d3, d2, d1, d0);
    end process;

    hex0 <= f_seg7(d0);
    hex1 <= f_seg7(d1);
    hex2 <= f_seg7(d2);
    hex3 <= f_seg7(d3);

end architecture rtl;
