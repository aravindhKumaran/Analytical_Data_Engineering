-- Getting Last Date
SET LAST_SOLD_DATE_SK = (SELECT MAX(SOLD_DATE_SK) FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES);

-- Removing partial records from the last date
DELETE FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES WHERE sold_date_sk=$LAST_SOLD_DATE_SK;

CREATE OR REPLACE TEMPORARY TABLE TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES_TMP AS (
WITH incremental_sales AS (
SELECT 
            CS_WAREHOUSE_SK AS warehouse_sk,
            CS_ITEM_SK AS item_sk,
            CS_SOLD_DATE_SK AS sold_date_sk,
            CS_QUANTITY AS quantity,
            cs_sales_price * cs_quantity AS sales_amt,
            CS_NET_PROFIT AS net_profit
    FROM tpcds.raw.catalog_sales
    WHERE sold_date_sk >= NVL($LAST_SOLD_DATE_SK,0) 
        AND quantity IS NOT NULL
        AND sales_amt IS NOT NULL
    
    UNION ALL

    SELECT 
            WS_WAREHOUSE_SK AS warehouse_sk,
            WS_ITEM_SK AS item_sk,
            WS_SOLD_DATE_SK AS sold_date_sk,
            WS_QUANTITY AS quantity,
            ws_sales_price * ws_quantity AS sales_amt,
            WS_NET_PROFIT AS net_profit
    FROM tpcds.raw.web_sales
    WHERE sold_date_sk >= NVL($LAST_SOLD_DATE_SK,0) 
        AND quantity IS NOT NULL
        AND sales_amt IS NOT NULL
),

-- aggregate at daily level
aggregating_records_to_daily_sales AS
(
SELECT 
    warehouse_sk,
    item_sk,
    sold_date_sk, 
    sum(quantity) AS daily_qty,
    sum(sales_amt) AS daily_sales_amt,
    sum(net_profit) AS daily_net_profit 
FROM incremental_sales
GROUP BY 1, 2, 3
),

adding_week_number_and_yr_number AS
(
SELECT 
    *,
    date.wk_num AS sold_wk_num,
    date.yr_num AS sold_yr_num
FROM aggregating_records_to_daily_sales 
LEFT JOIN tpcds.raw.date_dim date 
    ON sold_date_sk = d_date_sk
)
SELECT 
	warehouse_sk,
    item_sk,
    sold_date_sk,
    max(sold_wk_num) AS sold_wk_num,
    max(sold_yr_num) AS sold_yr_num,
    sum(daily_qty) AS daily_qty,
    sum(daily_sales_amt) AS daily_sales_amt,
    sum(daily_net_profit) AS daily_net_profit 
FROM adding_week_number_and_yr_number
GROUP BY 1,2,3
ORDER BY 1,2,3
);

-- Inserting new records
INSERT INTO TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES
(	
    WAREHOUSE_SK, 
    ITEM_SK, 
    SOLD_DATE_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    DAILY_QTY, 
    DAILY_SALES_AMT, 
    DAILY_NET_PROFIT
)
SELECT 
    DISTINCT
	warehouse_sk,
    item_sk,
    sold_date_sk,
    sold_wk_num,
    sold_yr_num,
    daily_qty,
    daily_sales_amt,
    daily_net_profit 
FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES_TMP;

