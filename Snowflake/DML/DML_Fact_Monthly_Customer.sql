-- setting last date
SET LAST_SOLD_DATE_SK = (SELECT MAX(SOLD_DATE_SK) FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES);

SET LAST_YR = (SELECT MIN(YR_NUM) FROM TPCDS.RAW.DATE_DIM WHERE D_DATE_SK >= NVL($LAST_SOLD_DATE_SK,0));

SET LAST_MTH = (SELECT MIN(MNTH_NUM) FROM TPCDS.RAW.DATE_DIM WHERE D_DATE_SK >= NVL($LAST_SOLD_DATE_SK,0) AND YR_NUM = NVL($LAST_YR,0));

-- creating tmp table
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

--select distinct * from TPCDS.ANALYTICS.MONTHLY_CUSTOMER_PROGRAM_TMP;

-- inserting new records
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

