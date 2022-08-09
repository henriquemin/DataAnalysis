-- Customizable SQL for Datespine -> helps to calculate average daily sales, or build a time series forecast model,
My_First_Table
sales_date	product	sales
2022-04-14	A	    46
2022-04-14	B	    409
2022-04-15	A	    17
2022-04-15	B	    480
2022-04-18	A	    65
2022-04-19	A	    45
2022-04-19	B	    411

WITH 
GLOBAL_SPINE AS 
(
  SELECT 
                    ROW_NUMBER() OVER (ORDER BY NULL)                                       as INTERVAL_ID, 
                    DATEADD('day', (INTERVAL_ID - 1), '2020-01-01T00:00' :: timestamp_ntz)  as SPINE_START, 
                    DATEADD('day', INTERVAL_ID, '2020-01-01T00:00' :: timestamp_ntz)        as SPINE_END 
  FROM              TABLE (GENERATOR(ROWCOUNT => 1097))
),

GROUPS AS 
(
  SELECT 
                    product, 
                    MIN(sales_date) AS LOCAL_START, 
                    MAX(sales_date) AS LOCAL_END 
  FROM              My_First_Table 
  GROUP BY          product
)

GROUP_SPINE AS 
(
  SELECT 
                    product, 
                    SPINE_START AS GROUP_START, 
                    SPINE_END AS GROUP_END 
  FROM              GROUPS G 
  CROSS JOIN        LATERAL 
                    (SELECT 
                        SPINE_START, 
                        SPINE_END 
                    FROM GLOBAL_SPINE S 
                    WHERE S.SPINE_START >= G.LOCAL_START
                    )
)

SELECT 
                     G.product AS GROUP_BY_product, 
                    GROUP_START, 
                    GROUP_END, 
                    T.* 
FROM                GROUP_SPINE G
LEFT JOIN           My_First_Table T 
ON                  sales_date >= G.GROUP_START 
AND                 sales_date < G.GROUP_END 
AND                 G.product = T.product
;

-- Market Basket Analysis
 When doing a market basket analysis or mining for association rules, the first step is often formatting the data to aggregate each transaction into a single record. This can be challenging for your laptop, but your data warehouse is designed to crunch this data efficiently.

Typical transaction data:

SALESORDERNUMBER	CUSTOMERKEY	ENGLISHPRODUCTNAME	LISTPRICE	WEIGHT	ORDERDATE
SO51247	11249	Mountain-200 Black	2294.99	23.77	1/1/2013
SO51247	11249	Water Bottle - 30 oz.	4.99		1/1/2013
SO51247	11249	Mountain Bottle Cage	9.99		1/1/2013
SO51246	25625	Sport-100 Helmet	34.99		12/31/2012
SO51246	25625	Water Bottle - 30 oz.	4.99		12/31/2012
SO51246	25625	Road Bottle Cage	8.99		12/31/2012
SO51246	25625	Touring-1000 Blue	2384.07	25.42	12/31/2012
 

Customizable SQL for Market Basket

WITH order_detail as (
  SELECT 
    SALESORDERNUMBER, 
    listagg(ENGLISHPRODUCTNAME, ', ') WITHIN group (
      order by 
        ENGLISHPRODUCTNAME
    ) as ENGLISHPRODUCTNAME_listagg, 
    COUNT(ENGLISHPRODUCTNAME) as num_products 
  FROM 
    transactions 
  GROUP BY 
    SALESORDERNUMBER
) 
SELECT 
  ENGLISHPRODUCTNAME_listagg, 
  count(SALESORDERNUMBER) as NumTransactions 
FROM 
  order_detail 
where 
  num_products > 1 
GROUP BY 
  ENGLISHPRODUCTNAME_listagg 
order by 
  count(SALESORDERNUMBER) desc;
  

-- Time-Series Aggregations
 

Time series aggregations are not only used by data scientists but they’re used for analytics as well. What makes them difficult is that window functions require the data to be formatted correctly.

For example, if you want to calculate the average sales amount in the past 14 days, window functions require you to have all sales data broken up into one row per day. Unfortunately, anyone who has worked with sales data before knows that it is usually stored at the transaction level. This is where time-series aggregation comes in handy. You can create aggregated, historical metrics without reformatting the entire dataset. It also comes in handy if we want to add multiple metrics at one time:

Average sales in the past 14 days
Biggest purchase in last 6 months
Count Distinct product types in last 90 days
If you wanted to use window functions, each metric would need to be built independently with several steps.

A better way to handle this, is to use common table expressions (CTEs) to define each of the historical windows, pre-aggregated.

For example:

Transaction ID	Customer ID	Product Type	Purchase Amt	Transaction Date
65432	101	Grocery	101.14	2022-03-01
65493	101	Grocery	98.45	2022-04-30
65494	101	Automotive	239.98	2022-05-01
66789	101	Grocery	86.55	2022-05-22
66981	101	Pharmacy	14	2022-06-15
67145	101	Grocery	93.12	2022-06-22
 

Customizable SQL for Time Series Aggregate SQL

WITH BASIC_OFFSET_14DAY AS (
  SELECT 
    A.CustomerID, 
    A.TransactionDate, 
    AVG(B.PurchaseAmount) as AVG_PURCHASEAMOUNT_PAST14DAY, 
    MAX(B.PurchaseAmount) as MAX_PURCHASEAMOUNT_PAST14DAY, 
    COUNT(DISTINCT B.TransactionID) as COUNT_DISTINCT_TRANSACTIONID_PAST14DAY
  FROM 
    My_First_Table A 
    INNER JOIN My_First_Table B ON A.CustomerID = B.CustomerID 
    AND 1 = 1 
  WHERE 
    B.TransactionDate >= DATEADD(day, -14, A.TransactionDate) 
    AND B.TransactionDate <= A.TransactionDate 
  GROUP BY 
    A.CustomerID, 
    A.TransactionDate
), 
BASIC_OFFSET_90DAY AS (
  SELECT 
    A.CustomerID, 
    A.TransactionDate, 
    AVG(B.PurchaseAmount) as AVG_PURCHASEAMOUNT_PAST90DAY, 
    MAX(B.PurchaseAmount) as MAX_PURCHASEAMOUNT_PAST90DAY, 
    COUNT(DISTINCT B.TransactionID) as COUNT_DISTINCT_TRANSACTIONID_PAST90DAY
  FROM 
    My_First_Table A 
    INNER JOIN My_First_Table B ON A.CustomerID = B.CustomerID 
    AND 1 = 1 
  WHERE 
    B.TransactionDate >= DATEADD(day, -90, A.TransactionDate) 
    AND B.TransactionDate <= A.TransactionDate 
  GROUP BY 
    A.CustomerID, 
    A.TransactionDate
), 
BASIC_OFFSET_180DAY AS (
  SELECT 
    A.CustomerID, 
    A.TransactionDate, 
    AVG(B.PurchaseAmount) as AVG_PURCHASEAMOUNT_PAST180DAY, 
    MAX(B.PurchaseAmount) as MAX_PURCHASEAMOUNT_PAST180DAY, 
    COUNT(DISTINCT B.TransactionID) as COUNT_DISTINCT_TRANSACTIONID_PAST180DAY
  FROM 
    My_First_Table A 
    INNER JOIN My_First_Table B ON A.CustomerID = B.CustomerID 
    AND 1 = 1 
  WHERE 
    B.TransactionDate >= DATEADD(day, -180, A.TransactionDate) 
    AND B.TransactionDate <= A.TransactionDate 
  GROUP BY 
    A.CustomerID, 
    A.TransactionDate
) 
SELECT 
  src.*, 
  BASIC_OFFSET_14DAY.AVG_PURCHASEAMOUNT_PAST14DAY, 
  BASIC_OFFSET_14DAY.MAX_PURCHASEAMOUNT_PAST14DAY, 
  BASIC_OFFSET_14DAY.COUNT_DISTINCT_TRANSACTIONID_PAST14DAY, 
  BASIC_OFFSET_90DAY.AVG_PURCHASEAMOUNT_PAST90DAY, 
  BASIC_OFFSET_90DAY.MAX_PURCHASEAMOUNT_PAST90DAY, 
  BASIC_OFFSET_90DAY.COUNT_DISTINCT_TRANSACTIONID_PAST90DAY, 
  BASIC_OFFSET_180DAY.AVG_PURCHASEAMOUNT_PAST180DAY, 
  BASIC_OFFSET_180DAY.MAX_PURCHASEAMOUNT_PAST180DAY, 
  BASIC_OFFSET_180DAY.COUNT_DISTINCT_TRANSACTIONID_PAST180DAY 
FROM 
  My_First_Table src 
  LEFT OUTER JOIN BASIC_OFFSET_14DAY ON BASIC_OFFSET_14DAY.TransactionDate = src.TransactionDate 
  AND BASIC_OFFSET_14DAY.CustomerID = src.CustomerID 
  LEFT OUTER JOIN BASIC_OFFSET_90DAY ON BASIC_OFFSET_90DAY.TransactionDate = src.TransactionDate 
  AND BASIC_OFFSET_90DAY.CustomerID = src.CustomerID 
  LEFT OUTER JOIN BASIC_OFFSET_180DAY ON BASIC_OFFSET_180DAY.TransactionDate = src.TransactionDate 
  AND BASIC_OFFSET_180DAY.CustomerID = src.CustomerID;
