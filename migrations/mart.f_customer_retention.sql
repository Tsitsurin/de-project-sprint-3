insert into mart.f_customer_retention (
	new_customers_count,
	returning_customers_count,
	refunded_customer_count,
	period_name,
	period_id,
	item_id,
	new_customers_revenue,
	returning_customers_revenue,
	customers_refunded)
select
	coalesce(new_cust.new_customers_count,0) as new_customers_count,
	ret_cust.returning_customers_count,
	coalesce(ref_c.refunded_customer_count,0) as refunded_customer_count,
	'weekly' as period_name,
	main.period_id,
	main.item_id,
	coalesce(new_cust.new_customers_revenue,0) as new_customers_revenue,
	ret_cust.returning_customers_revenue,
	coalesce(cust_ref.customers_refunded,0) as customers_refunded
from (
	select 
		s.item_id,
		dc.week_of_year as period_id
	from 
		mart.f_sales s
	join 
		mart.d_calendar dc on s.date_id = dc.date_id
	where 
		week_of_year = DATE_PART('week', '{{ds}}'::DATE)
	group by
		s.item_id,
		dc.week_of_year

		
	 ) main

left join 
	(
-- new_customers_count, new_customers_revenue
	select 
		count(customer_id) as new_customers_count,
		sum(payment_amount) as new_customers_revenue,
		dc.week_of_year as period_id, 
		item_id
	from 
		mart.f_sales sal
	join 
		mart.d_calendar dc on sal.date_id = dc.date_id
	where
		status = 'shipped' and
		week_of_year = DATE_PART('week', '{{ds}}'::DATE)
	group by 
		dc.week_of_year,
		item_id
	having 
		count(customer_id) = 1

	 ) new_cust on main.period_id = new_cust.period_id and main.item_id = new_cust.item_id

left join  
	 (
-- returning_customers_count, returning_customers_revenue
	select 
		count(*) as returning_customers_count,
		sum(customer_amount) as returning_customers_revenue,
		a.week_of_year as period_id,
		a.item_id
	from (
			select customer_id, dc.week_of_year, item_id, sum(payment_amount) as customer_amount
			from 
				mart.f_sales sal
			join 
				mart.d_calendar dc on sal.date_id = dc.date_id
			where 
				status = 'shipped' and 
				week_of_year = DATE_PART('week', '{{ds}}'::DATE)
			group by 
				customer_id,
				dc.week_of_year,
				item_id
			having 
				count(customer_id) > 1 
		 ) a
	group by
		a.week_of_year,
		a.item_id

	 ) ret_cust on main.period_id = ret_cust.period_id and main.item_id = ret_cust.item_id

left join 
	 (
-- refunded_customer_count
	select 
		count(*) as refunded_customer_count, 
		week_of_year as period_id,
		item_id
	from (
			select customer_id, dc.week_of_year, item_id
			from 
				mart.f_sales sal
			join 
				mart.d_calendar dc on sal.date_id = dc.date_id
			where 
				status = 'refunded' and
				week_of_year = DATE_PART('week', '{{ds}}'::DATE)
			group by 
				customer_id,
				dc.week_of_year,
				item_id
		 ) a
	group by
		week_of_year,
		item_id
	
	 ) ref_c on main.period_id = ref_c.period_id and main.item_id = ref_c.item_id

left join  
	 (
-- customers_refunded
	select
		count(*) as customers_refunded, 
		dc.week_of_year as period_id,
		item_id
	from 
		mart.f_sales sal
	join 
		mart.d_calendar dc on sal.date_id = dc.date_id
	where 
		status = 'refunded' and week_of_year = DATE_PART('week', '{{ds}}'::DATE)
	group by 
		dc.week_of_year,
		item_id
		
	 ) cust_ref on main.period_id = cust_ref.period_id and main.item_id = cust_ref.item_id
;