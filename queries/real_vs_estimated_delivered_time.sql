-- This query will return a table with the differences between the real 
-- and estimated delivery times by month and year. It will have different 
-- columns: 
-- month_no, with the month numbers going from 01 to 12;
-- month, with the 3 first letters of each month (e.g. Jan, Feb);
-- Year2016_real_time, with the average delivery time per month of 2016 (NaN if it doesn't exist);
-- Year2017_real_time, with the average delivery time per month of 2017 (NaN if it doesn't exist);
-- Year2018_real_time, with the average delivery time per month of 2018 (NaN if it doesn't exist);
-- Year2016_estimated_time, with the average estimated delivery time per month of 2016 (NaN if it doesn't exist); 
-- Year2017_estimated_time, with the average estimated delivery time per month of 2017 (NaN if it doesn't exist);
-- Year2018_estimated_time, with the average estimated delivery time per month of 2018 (NaN if it doesn't exist).

WITH time_stamps AS (
	SELECT
		strftime('%m', order_purchase_timestamp) AS month_no,
		strftime('%Y', order_purchase_timestamp) AS Year,
		JULIANDAY(order_delivered_customer_date) - JULIANDAY(order_purchase_timestamp) AS delivery_time, 
		JULIANDAY(order_estimated_delivery_date) - JULIANDAY(order_purchase_timestamp) AS estimated_delivery_time
	FROM olist_orders
	WHERE order_status = 'delivered' AND order_delivered_customer_date IS NOT NULL 
)
SELECT 
	month_no,
	SUBSTRING("--JanFebMarAprMayJunJulAugSepOctNovDec", month_no*3, 3) AS month,
	AVG(CASE WHEN Year = '2016' THEN delivery_time END) AS Year2016_real_time,
   	AVG(CASE WHEN Year = '2017' THEN delivery_time END) AS Year2017_real_time,
    AVG(CASE WHEN Year = '2018' THEN delivery_time END) AS Year2018_real_time,
    AVG(CASE WHEN Year = '2016' THEN estimated_delivery_time END) AS Year2016_estimated_time,
    AVG(CASE WHEN Year = '2017' THEN estimated_delivery_time END) AS Year2017_estimated_time,
    AVG(CASE WHEN Year = '2018' THEN estimated_delivery_time END) AS Year2018_estimated_time
FROM time_stamps
GROUP BY month_no;
