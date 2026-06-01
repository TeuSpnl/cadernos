--------------------------------------------------------------------------------
-- datapath.vhd
-- Lógica de dados: registradores de valor inserido, estoque, preço do produto,
-- soma de moedas, cálculo de troco e comparadores.
-- Todos os valores monetários em CENTAVOS (inteiro, máx. 9999 = R$ 99,99).
--------------------------------------------------------------------------------
library ieee;
    use ieee.std_logic_1164.all;
    use ieee.numeric_std.all;

entity datapath is
    generic (
        G_INIT_STOCK : positive := 3  -- unidades iniciais por produto
    );
    port (
        clk         : in  std_logic;
        rst_n       : in  std_logic;

        -- Índice do produto selecionado (0..4)
        prod_index  : in  std_logic_vector(2 downto 0);

        -- Controles vindos da FSM
        latch_prod      : in  std_logic;  -- trava produto e preço
        clr_inserted    : in  std_logic;  -- zera valor inserido
        en_add_coin     : in  std_logic;  -- habilita soma de moeda
        coin_value      : in  std_logic_vector(13 downto 0);  -- centavos
        dec_stock       : in  std_logic;  -- decrementa estoque após venda

        -- Status para a FSM
        stock_available : out std_logic;  -- estoque > 0 do produto latched
        payment_ok      : out std_logic;  -- inserido >= preço
        change_gt_zero  : out std_logic;  -- troco > 0

        -- Valores para exibição e devolução
        value_cents     : out std_logic_vector(13 downto 0);  -- inserido atual
        change_cents    : out std_logic_vector(13 downto 0)   -- troco calculado
    );
end entity datapath;

architecture rtl of datapath is

    constant C_MAX_CENTS : unsigned(13 downto 0) := to_unsigned(9999, 14);

    -- Preços em centavos por índice de produto
    type t_price_table is array (0 to 4) of unsigned(13 downto 0);
    constant C_PRICES : t_price_table := (
        0 => to_unsigned(250, 14),  -- Batata grande   R$ 2,50  SW[0]
        1 => to_unsigned(150, 14),  -- Batata média    R$ 1,50  SW[1]
        2 => to_unsigned( 75, 14),  -- Batata pequena  R$ 0,75  SW[2]
        3 => to_unsigned(350, 14),  -- Tortilha grande R$ 3,50  SW[3]
        4 => to_unsigned(200, 14)   -- Tortilha peq.   R$ 2,00  SW[4]
    );

    signal inserted_reg   : unsigned(13 downto 0);
    signal prod_lat       : unsigned(2 downto 0);
    signal price_reg      : unsigned(13 downto 0);
    signal change_sig     : unsigned(13 downto 0);

    -- Estoque: 3 bits por produto (0..7)
    type t_stock_array is array (0 to 4) of unsigned(2 downto 0);
    signal stock          : t_stock_array;
    signal stock_sel      : unsigned(2 downto 0);

    signal sum_next       : unsigned(13 downto 0);
    signal coin_u         : unsigned(13 downto 0);

begin

    coin_u <= unsigned(coin_value);

    -- -------------------------------------------------------------------------
    -- Registrador do valor inserido (limite R$ 99,99)
    -- -------------------------------------------------------------------------
    process (clk, rst_n)
    begin
        if rst_n = '0' then
            inserted_reg <= (others => '0');
        elsif rising_edge(clk) then
            if clr_inserted = '1' then
                inserted_reg <= (others => '0');
            elsif en_add_coin = '1' then
                -- Satura em 9999 centavos
                if sum_next > C_MAX_CENTS then
                    inserted_reg <= C_MAX_CENTS;
                else
                    inserted_reg <= sum_next;
                end if;
            end if;
        end if;
    end process;

    sum_next <= inserted_reg + coin_u;

    value_cents <= std_logic_vector(inserted_reg);

    -- -------------------------------------------------------------------------
    -- Latch do produto e preço selecionados
    -- -------------------------------------------------------------------------
    process (clk, rst_n)
        variable idx : integer;
    begin
        if rst_n = '0' then
            prod_lat  <= (others => '0');
            price_reg <= (others => '0');
        elsif rising_edge(clk) then
            if latch_prod = '1' then
                idx := to_integer(unsigned(prod_index));
                if idx >= 0 and idx <= 4 then
                    prod_lat  <= unsigned(prod_index);
                    price_reg <= C_PRICES(idx);
                end if;
            end if;
        end if;
    end process;

    -- -------------------------------------------------------------------------
    -- Estoque inicial (3 unidades cada) e decremento na venda
    -- -------------------------------------------------------------------------
    process (clk, rst_n)
        variable idx : integer;
    begin
        if rst_n = '0' then
            for i in 0 to 4 loop
                stock(i) <= to_unsigned(G_INIT_STOCK, 3);
            end loop;
        elsif rising_edge(clk) then
            if dec_stock = '1' then
                idx := to_integer(prod_lat);
                if idx >= 0 and idx <= 4 then
                    if stock(idx) > 0 then
                        stock(idx) <= stock(idx) - 1;
                    end if;
                end if;
            end if;
        end if;
    end process;

    -- Estoque do produto atualmente selecionado (latched)
    process (prod_lat, stock)
        variable idx : integer;
    begin
        idx := to_integer(prod_lat);
        if idx >= 0 and idx <= 4 then
            stock_sel <= stock(idx);
        else
            stock_sel <= (others => '0');
        end if;
    end process;

    stock_available <= '1' when stock_sel > 0 else '0';

    -- -------------------------------------------------------------------------
    -- Comparadores e troco
    -- -------------------------------------------------------------------------
    payment_ok     <= '1' when inserted_reg >= price_reg else '0';

    change_sig     <= inserted_reg - price_reg;
    change_cents   <= std_logic_vector(change_sig);
    change_gt_zero <= '1' when change_sig > 0 else '0';

end architecture rtl;
