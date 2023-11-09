INSERT INTO staging.user_order_log 
(uniq_id,
date_time,
city_id,
city_name,
customer_id,
first_name,
last_name,
item_id,
item_name,
quantity,
payment_amount,
status)
select 
	uniq_id,
	date_time,
	city_id,
	city_name,
	customer_id,
	first_name,
	last_name,
	item_id,
	item_name,
	quantity,
	case when status = 'refunded' then payment_amount*(-1) else payment_amount end as payment_amount,
	status
from 
	staging.pre_load_user_order_log
where 
    uniq_id not in (select uniq_id from staging.user_order_log );