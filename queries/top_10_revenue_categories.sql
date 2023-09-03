-- This query will return a table with the top 10 revenue categories in 
-- English, the number of orders and their total revenue. The first column will 
-- be Category, that will contain the top 10 revenue categories; the second one 
-- will be Num_order, with the total amount of orders of each category; and the 
-- last one will be Revenue, with the total revenue of each catgory.
-- HINT: All orders should have a delivered status and the Category and actual 
-- delivery date should be not null.


SELECT pcnt.product_category_name_english as Category,
        COUNT(DISTINCT o.order_id) AS Num_order,
        SUM(oop.payment_value) AS Revenue
FROM olist_orders o 
    JOIN olist_order_payments oop  ON oop.order_id =o.order_id
    JOIN olist_order_items ooi  ON o.order_id =ooi.order_id
    JOIN olist_products op ON op.product_id=ooi.product_id 
    JOIN product_category_name_translation pcnt ON op.product_category_name=pcnt.product_category_name 
WHERE o.order_status= 'delivered' AND o.order_delivered_customer_date is NOT NULL 
GROUP BY Category
ORDER BY Revenue DESC 
LIMIT 10

