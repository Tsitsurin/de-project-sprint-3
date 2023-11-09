alter table mart.f_sales add column if not exists status text;
alter table staging.user_order_log add column if not exists status text;