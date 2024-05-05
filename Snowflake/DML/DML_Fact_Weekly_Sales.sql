-- Getting Last Date
SET LAST_SOLD_WK_SK = (SELECT MAX(SOLD_WK_SK) FROM TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY);

-- Removing partial records from the last date
DELETE FROM TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY WHERE sold_wk_sk=$LAST_SOLD_WK_SK;

-- compiling all incremental sales records
CREATE OR REPLACE TEMPORARY TABLE TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY_TMP AS (
WITH aggregating_daily_sales_to_week AS (
SELECT 
    WAREHOUSE_SK, 
    ITEM_SK, 
    MIN(SOLD_DATE_SK) AS SOLD_WK_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    SUM(DAILY_QTY) AS SUM_QTY_WK, 
    SUM(DAILY_SALES_AMT) AS SUM_AMT_WK, 
    SUM(DAILY_NET_PROFIT) AS SUM_PROFIT_WK
FROM
    TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES
GROUP BY
    1,2,4,5
HAVING 
    sold_wk_sk >= NVL($LAST_SOLD_WK_SK,0)
),

finding_first_date_of_the_week AS (
SELECT 
    WAREHOUSE_SK, 
    ITEM_SK, 
    date.d_date_sk AS SOLD_WK_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    SUM_QTY_WK, 
    SUM_AMT_WK, 
    SUM_PROFIT_WK
FROM
    aggregating_daily_sales_to_week daily_sales
INNER JOIN TPCDS.RAW.DATE_DIM AS date
on daily_sales.SOLD_WK_NUM=date.wk_num
and daily_sales.sold_yr_num=date.yr_num
and date.day_of_wk_num=0
),

date_columns_in_inventory_table AS (
SELECT 
    inventory.*,
    date.wk_num AS inv_wk_num,
    date.yr_num AS inv_yr_num
FROM
    tpcds.RAW.inventory inventory
INNER JOIN TPCDS.RAW.DATE_DIM AS date
on inventory.inv_date_sk = date.d_date_sk
)

SELECT 
       warehouse_sk, 
       item_sk, 
       min(SOLD_WK_SK) AS sold_wk_sk,
       sold_wk_num AS sold_wk_num,
       sold_yr_num AS sold_yr_num,
       sum(sum_qty_wk) AS sum_qty_wk,
       sum(sum_amt_wk) AS sum_amt_wk,
       sum(sum_profit_wk) AS sum_profit_wk,
       sum(sum_qty_wk)/7 AS avg_qty_dy,
       sum(coalesce(inv.inv_quantity_on_hand, 0)) AS inv_qty_wk, 
       sum(coalesce(inv.inv_quantity_on_hand, 0)) / sum(sum_qty_wk) AS wks_sply,
       iff(avg_qty_dy>0 AND avg_qty_dy>inv_qty_wk, true , false) AS low_stock_flg_wk
FROM finding_first_date_of_the_week
LEFT JOIN date_columns_in_inventory_table inv 
    ON inv_wk_num = sold_wk_num AND inv_yr_num = sold_yr_num AND item_sk = inv_item_sk AND inv_warehouse_sk = warehouse_sk
GROUP BY 1, 2, 4, 5
HAVING SUM(sum_qty_wk) > 0
);

-- Inserting new records
INSERT INTO TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY
(	
	WAREHOUSE_SK, 
    ITEM_SK, 
    SOLD_WK_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    SUM_QTY_WK, 
    SUM_AMT_WK, 
    SUM_PROFIT_WK, 
    AVG_QTY_DY, 
    INV_QTY_WK, 
    WKS_SPLY, 
    LOW_STOCK_FLG_WK
    
)
SELECT 
    DISTINCT
	WAREHOUSE_SK, 
    ITEM_SK, 
    SOLD_WK_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    SUM_QTY_WK, 
    SUM_AMT_WK, 
    SUM_PROFIT_WK, 
    AVG_QTY_DY, 
    INV_QTY_WK, 
    WKS_SPLY, 
    LOW_STOCK_FLG_WK
FROM TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY_TMP;

-- SELECT * FROM TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY;

