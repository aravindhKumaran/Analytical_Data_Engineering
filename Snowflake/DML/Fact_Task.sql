-----------------------------------
--DAILY AGGREGATED SALES Procedure
CREATE OR REPLACE PROCEDURE TPCDS.INTERMEDIATE.populating_daily_aggregated_sales_incrementally()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
      DECLARE 
        LAST_SOLD_DATE_SK number;
    BEGIN
      SELECT MAX(SOLD_DATE_SK) INTO :LAST_SOLD_DATE_SK FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES;

DELETE FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES WHERE sold_date_sk=:LAST_SOLD_DATE_SK;

CREATE OR REPLACE TEMPORARY TABLE TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES_TMP AS (
with incremental_sales as (
SELECT 
            CS_WAREHOUSE_SK as warehouse_sk,
            CS_ITEM_SK as item_sk,
            CS_SOLD_DATE_SK as sold_date_sk,
            CS_QUANTITY as quantity,
            cs_sales_price * cs_quantity as sales_amt,
            CS_NET_PROFIT as net_profit
    from TPCDS.RAW.catalog_sales
    WHERE sold_date_sk >= NVL(:LAST_SOLD_DATE_SK,0) 
        and quantity is not null
        and sales_amt is not null
    
    union all

    SELECT 
            WS_WAREHOUSE_SK as warehouse_sk,
            WS_ITEM_SK as item_sk,
            WS_SOLD_DATE_SK as sold_date_sk,
            WS_QUANTITY as quantity,
            ws_sales_price * ws_quantity as sales_amt,
            WS_NET_PROFIT as net_profit
    from TPCDS.RAW.web_sales
    WHERE sold_date_sk >= NVL(:LAST_SOLD_DATE_SK,0) 
        and quantity is not null
        and sales_amt is not null
),

aggregating_records_to_daily_sales as
(
select 
    warehouse_sk,
    item_sk,
    sold_date_sk, 
    sum(quantity) as daily_qty,
    sum(sales_amt) as daily_sales_amt,
    sum(net_profit) as daily_net_profit 
from incremental_sales
group by 1, 2, 3

),

adding_week_number_and_yr_number as
(
select 
    *,
    date.wk_num as sold_wk_num,
    date.yr_num as sold_yr_num
from aggregating_records_to_daily_sales 
LEFT JOIN TPCDS.RAW.date_dim date 
    ON sold_date_sk = d_date_sk
)

SELECT 
	warehouse_sk,
    item_sk,
    sold_date_sk,
    max(sold_wk_num) as sold_wk_num,
    max(sold_yr_num) as sold_yr_num,
    sum(daily_qty) as daily_qty,
    sum(daily_sales_amt) as daily_sales_amt,
    sum(daily_net_profit) as daily_net_profit 
FROM adding_week_number_and_yr_number
GROUP BY 1,2,3
ORDER BY 1,2,3
);

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
  END
  $$;

-- Creating a scheduled task
  CREATE OR REPLACE TASK TPCDS.INTERMEDIATE.creating_daily_aggregated_sales_incrementally
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON * 8 * * * UTC'
    AS
CALL populating_daily_aggregated_sales_incrementally();

-- truncate table TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES;
-- ALTER TASK TPCDS.INTERMEDIATE.creating_daily_aggregated_sales_incrementally RESUME;
-- EXECUTE TASK TPCDS.INTERMEDIATE.creating_daily_aggregated_sales_incrementally;

-------------------------------------
-- WEEKLY AGGREGATED SALES Procedure
CREATE OR REPLACE PROCEDURE TPCDS.ANALYTICS.populating_weekly_aggregated_sales_incrementally()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
      DECLARE 
      LAST_SOLD_DATE_SK number;
    BEGIN
      SELECT MAX(SOLD_WK_SK) INTO :LAST_SOLD_DATE_SK FROM TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY;

      DELETE FROM TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY WHERE sold_wk_sk=:LAST_SOLD_WK_SK;

    CREATE OR REPLACE TEMPORARY TABLE TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY_TMP AS (
with aggregating_daily_sales_to_week as (
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
    sold_wk_sk >= NVL(:LAST_SOLD_WK_SK,0)
),

finding_first_date_of_the_week as (
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
INNER JOIN TPCDS.RAW.DATE_DIM as date
on daily_sales.SOLD_WK_NUM=date.wk_num
and daily_sales.sold_yr_num=date.yr_num
and date.day_of_wk_num=0
),

date_columns_in_inventory_table as (
SELECT 
    inventory.*,
    date.wk_num as inv_wk_num,
    date.yr_num as inv_yr_num
FROM
    TPCDS.RAW.inventory inventory
INNER JOIN TPCDS.RAW.DATE_DIM as date
on inventory.inv_date_sk = date.d_date_sk
)

select 
       warehouse_sk, 
       item_sk, 
       min(SOLD_WK_SK) as sold_wk_sk,
       sold_wk_num as sold_wk_num,
       sold_yr_num as sold_yr_num,
       sum(sum_qty_wk) as sum_qty_wk,
       sum(sum_amt_wk) as sum_amt_wk,
       sum(sum_profit_wk) as sum_profit_wk,
       sum(sum_qty_wk)/7 as avg_qty_dy,
       sum(coalesce(inv.inv_quantity_on_hand, 0)) as inv_qty_wk, 
       sum(coalesce(inv.inv_quantity_on_hand, 0)) / sum(sum_qty_wk) as wks_sply,
       iff(avg_qty_dy>0 and avg_qty_dy>inv_qty_wk, true , false) as low_stock_flg_wk
from finding_first_date_of_the_week
left join date_columns_in_inventory_table inv 
    on inv_wk_num = sold_wk_num and inv_yr_num = sold_yr_num and item_sk = inv_item_sk and inv_warehouse_sk = warehouse_sk
group by 1, 2, 4, 5
having sum(sum_qty_wk) > 0
);

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
  END
  $$;

-- Creating a scheduled task
CREATE OR REPLACE TASK TPCDS.ANALYTICS.creating_weekly_aggregated_sales_incrementally
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 9 * * 0 UTC'
    AS
CALL populating_weekly_aggregated_sales_incrementally();

-- truncate table TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY;
-- ALTER TASK TPCDS.ANALYTICS.creating_weekly_aggregated_sales_incrementally RESUME;
-- EXECUTE TASK TPCDS.ANALYTICS.creating_weekly_aggregated_sales_incrementally;

--------------------------------------
-- MONTHLY CUSTOMER PROGRAM Procedure
CREATE OR REPLACE PROCEDURE TPCDS.ANALYTICS.populating_monthly_customer_program_incrementally()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
    SET LAST_SOLD_DATE_SK = (SELECT MAX(SOLD_DATE_SK) FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES);
    
    SET LAST_YR = (SELECT MIN(YR_NUM) FROM TPCDS.RAW.DATE_DIM WHERE D_DATE_SK >= NVL($LAST_SOLD_DATE_SK,0));
    
    SET LAST_MTH = (SELECT MIN(MNTH_NUM) FROM TPCDS.RAW.DATE_DIM WHERE D_DATE_SK >= NVL($LAST_SOLD_DATE_SK,0) AND YR_NUM = NVL($LAST_YR,0));
    
    DELETE FROM TPCDS.ANALYTICS.CUSTOMER_MONTHLY_PROGRAM WHERE c_year=$LAST_YR AND c_month=$LAST_MTH;
    
    CREATE OR REPLACE TEMPORARY TABLE TPCDS.ANALYTICS.CUSTOMER_MONTHLY_PROGRAM_TMP AS (
    WITH sales AS (
        SELECT 
                cs_ship_customer_sk as customer_sk,
                CS_SOLD_DATE_SK AS sold_date_sk,
                CS_QUANTITY AS quantity,
                cs_sales_price * cs_quantity AS amount,
        FROM tpcds.raw.catalog_sales
        WHERE customer_sk IS NOT NULL AND quantity IS NOT NULL AND amount IS NOT NULL
        
        UNION ALL
    
        SELECT 
                ws_bill_customer_sk as customer_sk,
                WS_SOLD_DATE_SK AS sold_date_sk,
                WS_QUANTITY AS quantity,
                ws_sales_price * ws_quantity AS amount,
        FROM tpcds.raw.web_sales
        WHERE customer_sk IS NOT NULL AND quantity IS NOT NULL AND amount IS NOT NULL
    ),
    
    monthly_sales as (
    SELECT 
        customer_sk,
        yr_num as c_year,
        mnth_num as c_month,
        sum(quantity) over (partition by customer_sk order by yr_num, mnth_num) as sum_qty_mth,
        sum(amount) over (partition by customer_sk order by yr_num, mnth_num) as sum_amt_mth,
        avg(quantity) over (partition by customer_sk order by yr_num) as avg_qty_yr,
        avg(amount) over (partition by customer_sk order by yr_num) as avg_amt_yr,
        sum(amount) over (partition by customer_sk order by yr_num) as sum_amt_yr
    FROM sales
    JOIN TPCDS.RAW.DATE_DIM 
    ON sold_date_sk = d_date_sk
    ) 
    
    SELECT 
        customer_sk,
        c_first_name as first_name,
        c_last_name as last_name,
        c_customer_id as customer_id,
        c_email_address as email_address,
        c_year,
        c_month,
        sum_qty_mth, 
        sum_amt_mth,
        avg_qty_yr,
        avg_amt_yr,
        case when sum_amt_yr <= 25000 then 'Blue'
            when sum_amt_yr <= 50000 then 'Silver'
            when sum_amt_yr <= 100000 then 'Gold'
            when sum_amt_yr <= 200000 then 'Platinum'
            else 'Diamond' end as membership_status,
        iff(sum_amt_mth >= 100000, true , false) as prom_flg_mth
    FROM monthly_sales
    LEFT JOIN TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT 
    ON customer_sk = c_customer_sk
    HAVING c_year = NVL($LAST_YR,0) AND c_month = NVL($LAST_MTH,0)
    );
    
    INSERT INTO TPCDS.ANALYTICS.CUSTOMER_MONTHLY_PROGRAM ( 
        customer_sk,
        first_name,
        last_name,
        customer_id,
        email_address,
        c_year,
        c_month,
        sum_qty_mth,
        sum_amt_mth,
        avg_qty_yr,
        avg_amt_yr,
        membership_status,
        prom_flg_mth
    ) 
    SELECT 
        DISTINCT
        customer_sk,
        first_name,
        last_name,
        customer_id,
        email_address,
        c_year,
        c_month,
        sum_qty_mth,
        sum_amt_mth,
        avg_qty_yr,
        avg_amt_yr,
        membership_status,
        prom_flg_mth
    FROM TPCDS.ANALYTICS.CUSTOMER_MONTHLY_PROGRAM_TMP;
  END
  $$;

-- Creating a scheduled task
CREATE OR REPLACE TASK TPCDS.ANALYTICS.creating_monthly_customer_program_incrementally
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 0 21 * * UTC'
    AS
CALL populating_monthly_customer_program_incrementally();

-- truncate table TPCDS.ANALYTICS.CUSTOMER_MONTHLY_PROGRAM;
-- ALTER TASK TPCDS.ANALYTICS.creating_monthly_customer_program_incrementally RESUME;
-- EXECUTE TASK TPCDS.ANALYTICS.creating_monthly_customer_program_incrementally;

