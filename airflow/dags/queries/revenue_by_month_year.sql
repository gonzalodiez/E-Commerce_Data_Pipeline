-- This query will return a table with the revenue by month and year. It
-- will have different columns: 
-- month_no, with the month numbers going from 01 to 12;
-- month, with the 3 first letters of each month (e.g. Jan, Feb);
-- Year2016, with the revenue per month of 2016 (0.00 if it doesn't exist);
-- Year2017, with the revenue per month of 2017 (0.00 if it doesn't exist);
-- Year2018, with the revenue per month of 2018 (0.00 if it doesn't exist).


WITH temp_table AS (
	SELECT
		strftime('%m', o.order_delivered_customer_date) AS month_no,
		strftime('%Y', o.order_delivered_customer_date) AS YEAR,
		p.payment_value AS Payment
	from olist_orders O 
		JOIN  olist_order_payments P 
		ON O.order_id =P.order_id
	WHERE o.order_status = 'delivered' AND o.order_delivered_customer_date IS NOT NULL
	GROUP BY P.order_id 
	)
SELECT 
	month_no,
	SUBSTRING("--JanFebMarAprMayJunJulAugSepOctNovDec", month_no*3, 3) AS month,
	SUM(CASE WHEN Year = '2016' THEN Payment ELSE 0 END) AS Year2016,
   	SUM(CASE WHEN Year = '2017' THEN Payment ELSE 0 END) AS Year2017,
    SUM(CASE WHEN Year = '2018' THEN Payment ELSE 0 END) AS Year2018
FROM temp_table 
GROUP BY month_no
