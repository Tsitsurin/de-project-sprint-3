CREATE TABLE mart.f_customer_retention (
new_customers_count bigint,
returning_customers_count bigint,
refunded_customer_count bigint,
period_name text,
period_id bigint,
item_id bigint,
new_customers_revenue numeric(10,2),
returning_customers_revenue numeric(10,2),
customers_refunded bigint
);
