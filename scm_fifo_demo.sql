/*==============================================================================
  SCM FIFO DEMO — Bulk Purchase/Sales Matching using ANSI CTE, wrapped for
  Oracle Forms consumption (Forms cannot parse WITH clauses / ANSI joins
  directly in a block query, so we hide the logic behind a package + view).

  Analogy: same as Asset Management "FIFO redemption" — oldest purchase lot
  (NAV buy price) is depleted first against a sale/redemption (NAV sell price),
  and gain/loss = (sale_rate - purchase_rate) * qty_matched.
==============================================================================*/

-------------------------------------------------------------------------------
-- 1. TABLE STRUCTURE
-------------------------------------------------------------------------------

CREATE TABLE fifo_item_lots (
    lot_id         NUMBER        PRIMARY KEY,
    item_code      VARCHAR2(20)  NOT NULL,
    purchase_date  DATE          NOT NULL,
    qty_purchased  NUMBER(12,2)  NOT NULL,
    purchase_rate  NUMBER(12,4)  NOT NULL
);

CREATE TABLE fifo_sales_txn (
    sale_id        NUMBER        PRIMARY KEY,
    item_code      VARCHAR2(20)  NOT NULL,
    sale_date      DATE          NOT NULL,
    qty_sold       NUMBER(12,2)  NOT NULL,
    sale_rate      NUMBER(12,4)  NOT NULL
);

-------------------------------------------------------------------------------
-- 2. BULK SAMPLE DATA (two items, staggered purchase lots and sales)
-------------------------------------------------------------------------------

INSERT INTO fifo_item_lots VALUES (1, 'ITM-100', DATE '2026-01-05', 100, 50.00);
INSERT INTO fifo_item_lots VALUES (2, 'ITM-100', DATE '2026-01-12', 150, 52.50);
INSERT INTO fifo_item_lots VALUES (3, 'ITM-100', DATE '2026-01-20',  80, 49.00);
INSERT INTO fifo_item_lots VALUES (4, 'ITM-100', DATE '2026-02-02', 200, 55.00);
INSERT INTO fifo_item_lots VALUES (5, 'ITM-200', DATE '2026-01-08', 300, 12.00);
INSERT INTO fifo_item_lots VALUES (6, 'ITM-200', DATE '2026-01-25', 250, 12.75);
INSERT INTO fifo_item_lots VALUES (7, 'ITM-200', DATE '2026-02-10', 400, 11.50);

INSERT INTO fifo_sales_txn VALUES (101, 'ITM-100', DATE '2026-01-15', 120, 58.00);
INSERT INTO fifo_sales_txn VALUES (102, 'ITM-100', DATE '2026-01-28',  90, 57.25);
INSERT INTO fifo_sales_txn VALUES (103, 'ITM-100', DATE '2026-02-10', 100, 60.00);
INSERT INTO fifo_sales_txn VALUES (201, 'ITM-200', DATE '2026-01-18', 280, 13.20);
INSERT INTO fifo_sales_txn VALUES (202, 'ITM-200', DATE '2026-02-05', 200, 13.00);

COMMIT;

-------------------------------------------------------------------------------
-- 3. THE FIFO MATCHING LOGIC — pure ANSI CTE + analytic functions
--    (This is the query Forms CANNOT run directly. Cumulative-sum overlap
--     technique: turn each lot/sale into a [start_qty, end_qty) range on a
--     running total, then intersect ranges to get FIFO-matched quantity.)
-------------------------------------------------------------------------------

WITH lots AS (
    SELECT lot_id, item_code, purchase_date, purchase_rate, qty_purchased,
           SUM(qty_purchased) OVER (PARTITION BY item_code
                                     ORDER BY purchase_date, lot_id) AS cum_end,
           SUM(qty_purchased) OVER (PARTITION BY item_code
                                     ORDER BY purchase_date, lot_id)
             - qty_purchased AS cum_start
    FROM fifo_item_lots
),
sales AS (
    SELECT sale_id, item_code, sale_date, sale_rate, qty_sold,
           SUM(qty_sold) OVER (PARTITION BY item_code
                                ORDER BY sale_date, sale_id) AS cum_end,
           SUM(qty_sold) OVER (PARTITION BY item_code
                                ORDER BY sale_date, sale_id)
             - qty_sold AS cum_start
    FROM fifo_sales_txn
)
SELECT
    s.item_code,
    s.sale_id,
    s.sale_date,
    s.sale_rate,
    l.lot_id,
    l.purchase_date,
    l.purchase_rate,
    LEAST(s.cum_end, l.cum_end) - GREATEST(s.cum_start, l.cum_start)          AS qty_matched,
    (LEAST(s.cum_end, l.cum_end) - GREATEST(s.cum_start, l.cum_start))
        * (s.sale_rate - l.purchase_rate)                                     AS realized_gain_loss
FROM sales s
JOIN lots  l
  ON l.item_code = s.item_code
 AND GREATEST(s.cum_start, l.cum_start) < LEAST(s.cum_end, l.cum_end)
ORDER BY s.item_code, s.sale_id, l.lot_id;


-------------------------------------------------------------------------------
-- 4. PACKAGE — encapsulate the CTE behind a REF CURSOR interface
--    (Architecture-only scope: no Forms layer. A ref cursor can return the
--    CTE result set directly — no object type / pipelined-function detour
--    needed, since that was only ever required to expose the data through
--    a plain SQL view for a legacy-join-only caller.)
-------------------------------------------------------------------------------

CREATE OR REPLACE PACKAGE pkg_fifo_scm AS

    -- Core FIFO allocation: which lot(s) matched which sale, and the
    -- resulting realized gain/loss, filtered optionally by item.
    PROCEDURE get_fifo_allocation_cur(
        p_item_code IN  VARCHAR2 DEFAULT NULL,
        p_cursor    OUT SYS_REFCURSOR
    );

    -- Remaining (unconsumed) stock per lot, after FIFO matching.
    PROCEDURE get_remaining_stock_cur(
        p_item_code IN  VARCHAR2 DEFAULT NULL,
        p_cursor    OUT SYS_REFCURSOR
    );

    -- Summary: total realized gain/loss per item.
    PROCEDURE get_gain_loss_summary_cur(
        p_item_code IN  VARCHAR2 DEFAULT NULL,
        p_cursor    OUT SYS_REFCURSOR
    );

END pkg_fifo_scm;
/

CREATE OR REPLACE PACKAGE BODY pkg_fifo_scm AS

    PROCEDURE get_fifo_allocation_cur(
        p_item_code IN  VARCHAR2 DEFAULT NULL,
        p_cursor    OUT SYS_REFCURSOR
    ) IS
    BEGIN
        OPEN p_cursor FOR
            WITH lots AS (
                SELECT lot_id, item_code, purchase_date, purchase_rate, qty_purchased,
                       SUM(qty_purchased) OVER (PARTITION BY item_code
                                                 ORDER BY purchase_date, lot_id) AS cum_end,
                       SUM(qty_purchased) OVER (PARTITION BY item_code
                                                 ORDER BY purchase_date, lot_id)
                         - qty_purchased AS cum_start
                FROM fifo_item_lots
                WHERE p_item_code IS NULL OR item_code = p_item_code
            ),
            sales AS (
                SELECT sale_id, item_code, sale_date, sale_rate, qty_sold,
                       SUM(qty_sold) OVER (PARTITION BY item_code
                                            ORDER BY sale_date, sale_id) AS cum_end,
                       SUM(qty_sold) OVER (PARTITION BY item_code
                                            ORDER BY sale_date, sale_id)
                         - qty_sold AS cum_start
                FROM fifo_sales_txn
                WHERE p_item_code IS NULL OR item_code = p_item_code
            )
            SELECT
                s.item_code, s.sale_id, s.sale_date, s.sale_rate,
                l.lot_id, l.purchase_date, l.purchase_rate,
                LEAST(s.cum_end, l.cum_end) - GREATEST(s.cum_start, l.cum_start) AS qty_matched,
                (LEAST(s.cum_end, l.cum_end) - GREATEST(s.cum_start, l.cum_start))
                    * (s.sale_rate - l.purchase_rate) AS realized_gain_loss
            FROM sales s
            JOIN lots  l
              ON l.item_code = s.item_code
             AND GREATEST(s.cum_start, l.cum_start) < LEAST(s.cum_end, l.cum_end)
            ORDER BY s.item_code, s.sale_id, l.lot_id;
    END get_fifo_allocation_cur;

    PROCEDURE get_remaining_stock_cur(
        p_item_code IN  VARCHAR2 DEFAULT NULL,
        p_cursor    OUT SYS_REFCURSOR
    ) IS
    BEGIN
        OPEN p_cursor FOR
            WITH lots AS (
                SELECT lot_id, item_code, purchase_date, purchase_rate, qty_purchased,
                       SUM(qty_purchased) OVER (PARTITION BY item_code
                                                 ORDER BY purchase_date, lot_id) AS cum_end,
                       SUM(qty_purchased) OVER (PARTITION BY item_code
                                                 ORDER BY purchase_date, lot_id)
                         - qty_purchased AS cum_start
                FROM fifo_item_lots
                WHERE p_item_code IS NULL OR item_code = p_item_code
            ),
            sales AS (
                SELECT sale_id, item_code, sale_date, sale_rate, qty_sold,
                       SUM(qty_sold) OVER (PARTITION BY item_code
                                            ORDER BY sale_date, sale_id) AS cum_end,
                       SUM(qty_sold) OVER (PARTITION BY item_code
                                            ORDER BY sale_date, sale_id)
                         - qty_sold AS cum_start
                FROM fifo_sales_txn
                WHERE p_item_code IS NULL OR item_code = p_item_code
            ),
            consumed AS (
                SELECT l.lot_id,
                       SUM(LEAST(s.cum_end, l.cum_end) - GREATEST(s.cum_start, l.cum_start)) AS qty_consumed
                FROM lots l
                JOIN sales s
                  ON s.item_code = l.item_code
                 AND GREATEST(s.cum_start, l.cum_start) < LEAST(s.cum_end, l.cum_end)
                GROUP BY l.lot_id
            )
            SELECT l.lot_id, l.item_code, l.purchase_date, l.purchase_rate,
                   l.qty_purchased,
                   l.qty_purchased - NVL(c.qty_consumed, 0) AS qty_remaining
            FROM fifo_item_lots l
            LEFT JOIN consumed c ON c.lot_id = l.lot_id
            WHERE p_item_code IS NULL OR l.item_code = p_item_code
            ORDER BY l.item_code, l.purchase_date, l.lot_id;
    END get_remaining_stock_cur;

    PROCEDURE get_gain_loss_summary_cur(
        p_item_code IN  VARCHAR2 DEFAULT NULL,
        p_cursor    OUT SYS_REFCURSOR
    ) IS
    BEGIN
        OPEN p_cursor FOR
            WITH lots AS (
                SELECT lot_id, item_code, purchase_date, purchase_rate, qty_purchased,
                       SUM(qty_purchased) OVER (PARTITION BY item_code
                                                 ORDER BY purchase_date, lot_id) AS cum_end,
                       SUM(qty_purchased) OVER (PARTITION BY item_code
                                                 ORDER BY purchase_date, lot_id)
                         - qty_purchased AS cum_start
                FROM fifo_item_lots
                WHERE p_item_code IS NULL OR item_code = p_item_code
            ),
            sales AS (
                SELECT sale_id, item_code, sale_date, sale_rate, qty_sold,
                       SUM(qty_sold) OVER (PARTITION BY item_code
                                            ORDER BY sale_date, sale_id) AS cum_end,
                       SUM(qty_sold) OVER (PARTITION BY item_code
                                            ORDER BY sale_date, sale_id)
                         - qty_sold AS cum_start
                FROM fifo_sales_txn
                WHERE p_item_code IS NULL OR item_code = p_item_code
            ),
            alloc AS (
                SELECT s.item_code,
                       LEAST(s.cum_end, l.cum_end) - GREATEST(s.cum_start, l.cum_start) AS qty_matched,
                       (LEAST(s.cum_end, l.cum_end) - GREATEST(s.cum_start, l.cum_start))
                           * (s.sale_rate - l.purchase_rate) AS realized_gain_loss
                FROM sales s
                JOIN lots  l
                  ON l.item_code = s.item_code
                 AND GREATEST(s.cum_start, l.cum_start) < LEAST(s.cum_end, l.cum_end)
            )
            SELECT item_code,
                   SUM(qty_matched)        AS total_qty_sold,
                   SUM(realized_gain_loss) AS total_realized_gain_loss
            FROM alloc
            GROUP BY item_code
            ORDER BY item_code;
    END get_gain_loss_summary_cur;

END pkg_fifo_scm;
/

-------------------------------------------------------------------------------
-- 5. HOW TO CALL IT (PL/SQL block using the ref cursor — pure architecture
--    test, no Forms involved)
-------------------------------------------------------------------------------

DECLARE
    v_cur SYS_REFCURSOR;
    v_item_code           fifo_item_lots.item_code%TYPE;
    v_sale_id             fifo_sales_txn.sale_id%TYPE;
    v_sale_date           fifo_sales_txn.sale_date%TYPE;
    v_sale_rate           fifo_sales_txn.sale_rate%TYPE;
    v_lot_id              fifo_item_lots.lot_id%TYPE;
    v_purchase_date       fifo_item_lots.purchase_date%TYPE;
    v_purchase_rate       fifo_item_lots.purchase_rate%TYPE;
    v_qty_matched         NUMBER;
    v_gain_loss            NUMBER;
BEGIN
    pkg_fifo_scm.get_fifo_allocation_cur('ITM-100', v_cur);
    LOOP
        FETCH v_cur INTO v_item_code, v_sale_id, v_sale_date, v_sale_rate,
                          v_lot_id, v_purchase_date, v_purchase_rate,
                          v_qty_matched, v_gain_loss;
        EXIT WHEN v_cur%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE(
            'Sale ' || v_sale_id || ' <- Lot ' || v_lot_id ||
            ' | Qty: ' || v_qty_matched ||
            ' | Gain/Loss: ' || v_gain_loss
        );
    END LOOP;
    CLOSE v_cur;
END;
/
