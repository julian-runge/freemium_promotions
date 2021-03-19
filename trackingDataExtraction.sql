-- CROSS-SECTIONAL DATASET OF PRICE PROMOTION EXPERIMENT
drop table x_xsection;

-- GET WHAT WE CAN FROM USER_BASE
create table x_xsection as
select
	unique_id,
	created_date install_date,
	created_ts install_ts,
	TO_CHAR(created_date,'DAY') weekday_install,
	country,
	mobile_source,
	mobile_source_detail,
	device,
	device_model,
	initial_language,
	latest_language
from
	x.user_base
where
	created_date between '2016-12-15' and '2017-02-22'
;

-- GET DEVICE INFO
-- RAM (HISTORICALLY THE BEST CORRELATE OF DEVICE PRICE AND USER SPENDING)
alter table x_xsection add column device_ram int;

merge into x_xsection a using
(select		
	device,	
	max(tracking_ram) device_ram
from		
	ACROSS_GAMES.ANDROID_DEVICES_FULL
group by
	1
) b
on (a.device=b.device)
when matched then update set a.device_ram=b.device_ram
;

-- IMPUTE MEAN FOR MISSING VALUES
update x_xsection set device_ram=(select round(avg(device_ram),0) from x_xsection) where device_ram is null;


-- X RESOLUTION
alter table x_xsection add column x_resolution int;

merge into x_xsection a using
(select		
	device,	
	max(tracking_x_resolution) x_resolution
from		
	ACROSS_GAMES.ANDROID_DEVICES_FULL
group by
	1
) b
on (a.device=b.device)
when matched then update set a.x_resolution=b.x_resolution
;

-- Y RESOLUTION
alter table x_xsection add column y_resolution int;

merge into x_xsection a using
(select		
	device,	
	max(tracking_x_resolution) y_resolution
from		
	ACROSS_GAMES.ANDROID_DEVICES_FULL
group by
	1
) b
on (a.device=b.device)
when matched then update set a.y_resolution=b.y_resolution
;

-- DPI
alter table x_xsection add column dpi int;

merge into x_xsection a using
(select		
	device,	
	max(tracking_dpi) dpi
from		
	ACROSS_GAMES.ANDROID_DEVICES_FULL
group by
	1
) b
on (a.device=b.device)
when matched then update set a.dpi=b.dpi
;

-- AVG SPENDING OF USERS ON SAME DEVICE IN OTHER LARGE PORTFOLIO GAMES
alter table x_xsection add column y1_d30devrev decimal(12,5);
update x_xsection set y1_d30devrev=0;

merge into x_xsection a using
(select		
	device,	
	avg(coalesce(d30_bookings_usd,0)) d30devrev
from		
	y1.user_base
where
	created_date<add_days(curdate(),-30)
group by
	1
) b
on (a.device=b.device)
when matched then update set a.y1_d30devrev=b.d30devrev
;

alter table x_xsection add column y2_d30devrev decimal(12,5);
update x_xsection set y2_d30devrev=0;

merge into x_xsection a using
(select		
	device,	
	avg(coalesce(d30_bookings_usd,0)) d30devrev
from		
	y2.user_base
where
	created_date<add_days(curdate(),-30)
group by
	1
) b
on (a.device=b.device)
when matched then update set a.y2_d30devrev=b.d30devrev
;

alter table x_xsection add column y3_d30devrev decimal(12,5);
update x_xsection set y3_d30devrev=0;

merge into x_xsection a using
(select		
	device,	
	avg(coalesce(d30_bookings_usd,0)) d30devrev
from		
	y3.user_base
where
	created_date<add_days(curdate(),-30)
group by
	1
) b
on (a.device=b.device)
when matched then update set a.y3_d30devrev=b.d30devrev
;

alter table x_xsection add column y4_d30devrev decimal(12,5);
update x_xsection set y4_d30devrev=0;

merge into x_xsection a using
(select		
	device,	
	avg(coalesce(d30_bookings_usd,0)) d30devrev
from		
	y4.user_base
where
	created_date<add_days(curdate(),-30)
group by
	1
) b
on (a.device=b.device)
when matched then update set a.y4_d30devrev=b.d30devrev
;

alter table x_xsection add column portfolio_d30devrev decimal(12,5);
update x_xsection set portfolio_d30devrev=0;
update x_xsection set portfolio_d30devrev=(coalesce(y4_d30devrev,0)+coalesce(y1_d30devrev,0)+coalesce(y2_d30devrev,0)+coalesce(y3_d30devrev,0))/4;
alter table x_xsection drop column y4_d30devrev;
alter table x_xsection drop column y2_d30devrev;
alter table x_xsection drop column y1_d30devrev;
alter table x_xsection drop column y3_d30devrev;

-- SPENDING OF USERS IN OTHER LARGE PORTFOLIO GAMES
alter table x_xsection add column y1_d30rev decimal(12,5);
update x_xsection set y1_d30rev=0;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(d30_bookings_usd,0)) d30rev
from		
	y1.user_base
where
	created_date<add_days(curdate(),-30)
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.y1_d30rev=b.d30rev
;

alter table x_xsection add column y2_d30rev decimal(12,5);
update x_xsection set y2_d30rev=0;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(d30_bookings_usd,0)) d30rev
from		
	y2.user_base
where
	created_date<add_days(curdate(),-30)
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.y2_d30rev=b.d30rev
;

alter table x_xsection add column y3_d30rev decimal(12,5);
update x_xsection set y3_d30rev=0;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(d30_bookings_usd,0)) d30rev
from		
	y3.user_base
where
	created_date<add_days(curdate(),-30)
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.y3_d30rev=b.d30rev
;

alter table x_xsection add column y4_d30rev decimal(12,5);
update x_xsection set y4_d30rev=0;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(d30_bookings_usd,0)) d30rev
from		
	y4.user_base
where
	created_date<add_days(curdate(),-30)
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.y4_d30rev=b.d30rev
;

alter table x_xsection add column portfolio_d30rev decimal(12,5);
update x_xsection set portfolio_d30rev=0;
update x_xsection set portfolio_d30rev=coalesce(y4_d30rev,0)+coalesce(y1_d30rev,0)+coalesce(y2_d30rev,0)+coalesce(y3_d30rev,0);
alter table x_xsection drop column y4_d30rev;
alter table x_xsection drop column y2_d30rev;
alter table x_xsection drop column y1_d30rev;
alter table x_xsection drop column y3_d30rev;

-- ENGAGEMENT OF USERS IN OTHER LARGE PORTFOLIO GAMES

-- ROUNDS PLAYED
alter table x_xsection add column y1_d30rounds int;
update x_xsection set y1_d30rounds=0;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(d30_rounds_played,0)) d30rounds
from		
	y1.user_base
where
	created_date<add_days(curdate(),-30)
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.y1_d30rounds=b.d30rounds
;

alter table x_xsection add column y2_d30rounds int;
update x_xsection set y2_d30rounds=0;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(d30_rounds_played,0)) d30rounds
from		
	y2.user_base
where
	created_date<add_days(curdate(),-30)
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.y2_d30rounds=b.d30rounds
;

alter table x_xsection add column y3_d30rounds int;
update x_xsection set y3_d30rounds=0;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(d30_rounds_played,0)) d30rounds
from		
	y3.user_base
where
	created_date<add_days(curdate(),-30)
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.y3_d30rounds=b.d30rounds
;

alter table x_xsection add column y4_d30rounds int;
update x_xsection set y4_d30rounds=0;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(d30_rounds_played,0)) d30rounds
from		
	y4.user_base
where
	created_date<add_days(curdate(),-30)
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.y4_d30rounds=b.d30rounds
;

alter table x_xsection add column portfolio_d30rounds int;
update x_xsection set portfolio_d30rounds=0;
update x_xsection set portfolio_d30rounds=(coalesce(y4_d30rounds,0)+coalesce(y1_d30rounds,0)+coalesce(y2_d30rounds,0)+coalesce(y3_d30rounds,0));
alter table x_xsection drop column y4_d30rounds;
alter table x_xsection drop column y2_d30rounds;
alter table x_xsection drop column y1_d30rounds;
alter table x_xsection drop column y3_d30rounds;

-- GET LEVEL 2 UNLOCK / LEVEL 1 COMPLETE TIMESTAMP
alter table x_xsection add column lvl1_complete_ts timestamp;

merge into x_xsection a using
(select		
	unique_id,	
	min(created_ts) lvl1_complete_ts	
from		
	x.level_unlock
where		
	right(level_id,2)='02'
	and created_date between '2016-12-15' and '2017-08-22'
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.lvl1_complete_ts=b.lvl1_complete_ts
;

merge into x_xsection a using
(select		
	unique_id,	
	min(created_ts) lvl1_complete_ts	
from		
	x.sessions
where		
	right(max_level_id,2)='02'
	and created_date between '2016-12-15' and '2017-08-22'
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.lvl1_complete_ts=b.lvl1_complete_ts
	where a.lvl1_complete_ts>=b.lvl1_complete_ts or a.lvl1_complete_ts is null
;

-- GET LEVEL 2 OR HIGHER UNLOCK / ACCESS TO PRICE PROMOTIONS
-- (TRACKING FOR UNLOCKING WAS INCOMPLETE, HENCE SOME USERS HAVE UNLOCKS FOR LEVEL 3 OR HIGHER, BUT NOT FOR LEVEL 2)
alter table x_xsection add column lvl1plus_complete_ts timestamp;
update x_xsection set lvl1plus_complete_ts=null;

merge into x_xsection a using
(select		
	unique_id,	
	min(created_ts) lvl1plus_complete_ts	
from		
	x.level_unlock	
where		
	right(level_id,2)>='02'
	and created_date between '2016-12-15' and '2017-08-22'
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.lvl1plus_complete_ts=b.lvl1plus_complete_ts
;

merge into x_xsection a using
(select		
	unique_id,	
	min(created_ts) lvl1plus_complete_ts	
from		
	x.sessions	
where		
	right(max_level_id,2)>='02'
	and created_date between '2016-12-15' and '2017-08-22'
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.lvl1plus_complete_ts=b.lvl1plus_complete_ts
	where a.lvl1plus_complete_ts>=b.lvl1plus_complete_ts or a.lvl1plus_complete_ts is null
;

-- SET LEVEL 1 COMPLETE TS TO LOWER VALUE OF THE TWO (DOES NOT ACTUALLY CHANGE ANY ENTRIES AND WE HENCE OBSERVE LEVEL 2 UNLOCK FOR ALL USERS)
update x_xsection set lvl1_complete_ts=lvl1plus_complete_ts where lvl1_complete_ts>lvl1plus_complete_ts;

-- SET ALL ENTRIES THAT DID NOT HAPPEN WITHIN OBSERVATION PERIOD TO 0
update x_xsection set lvl1_complete_ts=null where days_between(to_date(lvl1_complete_ts),to_date(install_ts))>180;

-- GET TREATMENT INFO IN THERE
alter table x_xsection add column treatment varchar(25);
update x_xsection set treatment='unclean';

update x_xsection set treatment='after0days' where (mod(unique_id,577) between 107 and 157 or mod(unique_id,577) between 431 and 576);
update x_xsection set treatment='after25days' where mod(unique_id,577) between 158 and 347;
update x_xsection set treatment='after50days' where (mod(unique_id,577) between 0 and 53 or mod(unique_id,577) between 391 and 430);
update x_xsection set treatment='no_promo' where (mod(unique_id,577) between 54 and 106 or mod(unique_id,577) between 348 and 390);
  	
-- GET USER BEHAVIOR DURING LEVEL ONE (PRE-TREATMENT) IN THERE

-- CALENDAR DAYS (NOT NECESSARILY WITH LOGIN/ACTIVITY) NEEDED TO COMPLETE LEVEL 1
alter table x_xsection add column days_complete_lvl1 int;
update x_xsection set days_complete_lvl1=days_between(to_date(lvl1_complete_ts),install_date);

-- DAYS ACTIVE
alter table x_xsection add column active_days_lvl1 int;

merge into x_xsection a using
(select		
	c.unique_id unique_id,
	count(distinct c.created_date) active_days_lvl1
from		
	x.app_logins c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and coalesce(d.lvl1_complete_ts,add_days(d.install_ts,180))
group by		
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.active_days_lvl1=b.active_days_lvl1 where lvl1_complete_ts is not null
;

-- SESSIONS
alter table x_xsection add column sessions_lvl1 int;

merge into x_xsection a using
(select		
	c.unique_id unique_id,
	count(c.unique_id) sessions_lvl1
from		
	x.app_logins c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and coalesce(d.lvl1_complete_ts,add_days(d.install_ts,180))
group by		
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.sessions_lvl1=b.sessions_lvl1 where lvl1_complete_ts is not null
;

-- ACTIVE HOURS
alter table x_xsection add column active_hours_lvl1 int;

merge into x_xsection a using
(select		
	c.unique_id unique_id,
	count(distinct concat(created_date,hour(created_ts))) active_hours_lvl1
from		
	x.app_logins c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and coalesce(d.lvl1_complete_ts,add_days(d.install_ts,180))
group by		
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.active_hours_lvl1=b.active_hours_lvl1 where lvl1_complete_ts is not null
;

-- SECONDS IN GAME
alter table x_xsection add column seconds_lvl1 bigint;

merge into x_xsection a using
(select		
	c.unique_id unique_id,
	sum(case when c.session_length<0 then 0 else c.session_length end) seconds_lvl1
from		
	(select
		created_date,
		created_ts,
		unique_id,
		lead(last_session_length,1) over(partition by unique_id order by created_ts) session_length
	from
		x.sessions
	where
		created_date between '2016-12-15' and '2017-08-23') c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and coalesce(d.lvl1_complete_ts,add_days(d.install_ts,180))
group by		
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.seconds_lvl1=b.seconds_lvl1 where lvl1_complete_ts is not null
;

-- ROUNDS
alter table x_xsection add column rounds_lvl1 int;

merge into x_xsection a using
(select		
	c.unique_id unique_id,
	count(c.unique_id) rounds_lvl1
from		
	ppa_scene_end c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and coalesce(d.lvl1_complete_ts,add_days(d.install_ts,180))
group by		
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.rounds_lvl1=b.rounds_lvl1 where lvl1_complete_ts is not null
;

-- ADS WATCHED
alter table x_xsection add column ads_lvl1 int;
update x_xsection set ads_lvl1=0;

merge into x_xsection a using
(select		
	c.unique_id unique_id,
	count(c.unique_id) ads_lvl1
from		
	ppa_ads c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and coalesce(d.lvl1_complete_ts,add_days(d.install_ts,180))
	and c.w_action='watch_ad'
group by		
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.ads_lvl1=b.ads_lvl1 where lvl1_complete_ts is not null
;

-- PURCHASES
alter table x_xsection add column purchases_lvl1 int;

merge into x_xsection a using
(select		
	c.unique_id unique_id,
	count(c.unique_id) purchases_lvl1
from		
	bookings.bookings_x c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and coalesce(d.lvl1_complete_ts,add_days(d.install_ts,180))
group by		
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.purchases_lvl1=b.purchases_lvl1 where lvl1_complete_ts is not null
;

-- REVENUE
alter table x_xsection add column revenue_lvl1 decimal(12,5);

merge into x_xsection a using
(select		
	c.unique_id unique_id,
	sum(c.bookings_usd) revenue_lvl1
from		
	bookings.bookings_x c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and coalesce(d.lvl1_complete_ts,add_days(d.install_ts,180))
group by		
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.revenue_lvl1=b.revenue_lvl1 where lvl1_complete_ts is not null
;

-- COINS BOUGHT AT NORMAL PRICE
alter table x_xsection add column coins_bought_lvl1 int;

merge into x_xsection a using
(select
	c.unique_id,
	sum(c.quantity) coins_bought_lvl1
from
	bookings.bookings_x c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and coalesce(d.lvl1_complete_ts,add_days(d.install_ts,180))
	and item='coins'
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.coins_bought_lvl1=b.coins_bought_lvl1 where lvl1_complete_ts is not null
;

-- CASH BOUGHT AT NORMAL PRICE
alter table x_xsection add column cash_bought_lvl1 int;

merge into x_xsection a using
(select
	c.unique_id,
	sum(c.quantity) cash_bought_lvl1
from
	bookings.bookings_x c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and coalesce(d.lvl1_complete_ts,add_days(d.install_ts,180))
	and item='cash'
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.cash_bought_lvl1=b.cash_bought_lvl1 where lvl1_complete_ts is not null
;

-- COINS SPENT
alter table x_xsection add column coins_spent_lvl1 int;

merge into x_xsection a using
(select
	c.unique_id,
	sum(c.buy_price) coins_spent_lvl1
from
	x.purchases c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and coalesce(d.lvl1_complete_ts,add_days(d.install_ts,180))
	and cash_coins='coins'
group by		
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.coins_spent_lvl1=b.coins_spent_lvl1 where lvl1_complete_ts is not null
;

-- CASH SPENT
alter table x_xsection add column cash_spent_lvl1 int;

merge into x_xsection a using
(select
	c.unique_id,
	sum(c.buy_price) cash_spent_lvl1
from
	x.purchases c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and coalesce(d.lvl1_complete_ts,add_days(d.install_ts,180))
	and cash_coins='cash'
group by		
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.cash_spent_lvl1=b.cash_spent_lvl1 where lvl1_complete_ts is not null
;

-- MIN COIN STOCK
alter table x_xsection add column min_coins_lvl1 int;

merge into x_xsection a using
(select
	c.unique_id,
	min(c.coin_count_pit) min_coins_lvl1
from
	x.sessions c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and coalesce(d.lvl1_complete_ts,add_days(d.install_ts,180))
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.min_coins_lvl1=b.min_coins_lvl1 where lvl1_complete_ts is not null
;

-- MIN CASH STOCK
alter table x_xsection add column min_cash_lvl1 int;

merge into x_xsection a using
(select
	c.unique_id,
	min(c.cash_count_pit) min_cash_lvl1
from
	x.sessions c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and coalesce(d.lvl1_complete_ts,add_days(d.install_ts,180))
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.min_cash_lvl1=b.min_cash_lvl1 where lvl1_complete_ts is not null
;

-- MAX COIN STOCK
alter table x_xsection add column max_coins_lvl1 int;

merge into x_xsection a using
(select
	c.unique_id,
	max(c.coin_count_pit) max_coins_lvl1
from
	x.sessions c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and coalesce(d.lvl1_complete_ts,add_days(d.install_ts,180))
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.max_coins_lvl1=b.max_coins_lvl1 where lvl1_complete_ts is not null
;

-- MAX CASH STOCK
alter table x_xsection add column max_cash_lvl1 int;

merge into x_xsection a using
(select
	c.unique_id,
	max(c.cash_count_pit) max_cash_lvl1
from
	x.sessions c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and coalesce(d.lvl1_complete_ts,add_days(d.install_ts,180))
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.max_cash_lvl1=b.max_cash_lvl1 where lvl1_complete_ts is not null
;

-- AVG COIN STOCK
alter table x_xsection add column avg_coins_lvl1 int;

merge into x_xsection a using
(select
	c.unique_id,
	round(avg(c.coin_count_pit),0) avg_coins_lvl1
from
	x.sessions c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and coalesce(d.lvl1_complete_ts,add_days(d.install_ts,180))
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.avg_coins_lvl1=b.avg_coins_lvl1 where lvl1_complete_ts is not null
;

-- AVG CASH STOCK
alter table x_xsection add column avg_cash_lvl1 int;

merge into x_xsection a using
(select
	c.unique_id,
	round(avg(c.cash_count_pit),0) avg_cash_lvl1
from
	x.sessions c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and coalesce(d.lvl1_complete_ts,add_days(d.install_ts,180))
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.avg_cash_lvl1=b.avg_cash_lvl1 where lvl1_complete_ts is not null
;


-- MEDIAN COIN STOCK
alter table x_xsection add column median_coins_lvl1 int;

merge into x_xsection a using
(select
	c.unique_id,
	median(c.coin_count_pit) median_coins_lvl1
from
	x.sessions c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and coalesce(d.lvl1_complete_ts,add_days(d.install_ts,180))
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.median_coins_lvl1=b.median_coins_lvl1 where lvl1_complete_ts is not null
;

-- MEDIAN CASH STOCK
alter table x_xsection add column median_cash_lvl1 int;

merge into x_xsection a using
(select
	c.unique_id,
	median(c.cash_count_pit) median_cash_lvl1
from
	x.sessions c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and coalesce(d.lvl1_complete_ts,add_days(d.install_ts,180))
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.median_cash_lvl1=b.median_cash_lvl1 where lvl1_complete_ts is not null
;

-- OUTCOMES UNTIL DAY 180
-- MIN COIN STOCK
alter table x_xsection add column min_coins int;

merge into x_xsection a using
(select
	c.unique_id,
	min(c.coin_count_pit) min_coins
from
	x.sessions c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and add_days(d.install_ts,180)
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.min_coins=b.min_coins
;

-- MIN CASH STOCK
alter table x_xsection add column min_cash int;

merge into x_xsection a using
(select
	c.unique_id,
	min(c.cash_count_pit) min_cash
from
	x.sessions c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and add_days(d.install_ts,180)
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.min_cash=b.min_cash
;

-- MAX COIN STOCK
alter table x_xsection add column max_coins int;

merge into x_xsection a using
(select
	c.unique_id,
	max(c.coin_count_pit) max_coins
from
	x.sessions c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and add_days(d.install_ts,180)
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.max_coins=b.max_coins
;

-- MAX CASH STOCK
alter table x_xsection add column max_cash int;

merge into x_xsection a using
(select
	c.unique_id,
	max(c.cash_count_pit) max_cash
from
	x.sessions c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and add_days(d.install_ts,180)
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.max_cash=b.max_cash
;

-- AVG COIN STOCK
alter table x_xsection add column avg_coins int;

merge into x_xsection a using
(select
	c.unique_id,
	round(avg(c.coin_count_pit),0) avg_coins
from
	x.sessions c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and add_days(d.install_ts,180)
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.avg_coins=b.avg_coins
;

-- AVG CASH STOCK
alter table x_xsection add column avg_cash int;

merge into x_xsection a using
(select
	c.unique_id,
	round(avg(c.cash_count_pit),0) avg_cash
from
	x.sessions c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and add_days(d.install_ts,180)
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.avg_cash=b.avg_cash
;


-- MEDIAN COIN STOCK
alter table x_xsection add column median_coins int;

merge into x_xsection a using
(select
	c.unique_id,
	median(c.coin_count_pit) median_coins
from
	x.sessions c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and add_days(d.install_ts,180)
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.median_coins=b.median_coins
;

-- MEDIAN CASH STOCK
alter table x_xsection add column median_cash int;

merge into x_xsection a using
(select
	c.unique_id,
	median(c.cash_count_pit) median_cash
from
	x.sessions c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and add_days(d.install_ts,180)
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.median_cash=b.median_cash
;

-- CONVERSION
alter table x_xsection add column conversion_ts timestamp;

merge into x_xsection a using
(select		
	unique_id,
	min(created_ts) conversion_ts	
from		
	bookings.bookings_x
where
	created_date between '2016-12-15' and '2017-08-22'
group by		
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.conversion_ts=b.conversion_ts
;

-- TOTAL BADGES EARNED (UP TO 5 BADGES CAN BE EARNED PER SCENE PLAYED)
alter table x_xsection add column total_badges int;

merge into x_xsection a using
(select
	c.unique_id,
	max(c.badge_count_pit) total_badges
from
	ppa_scene_end c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and add_days(d.install_ts,180)
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.total_badges=b.total_badges
;

-- DISTINCT SCENES PLAYED
alter table x_xsection add column distinct_scenes_played int;

merge into x_xsection a using
(select
	c.unique_id,
	count(distinct c.scene_id) distinct_scenes_played
from
	ppa_scene_end c
	join
	x_xsection d
	on c.unique_id=d.unique_id
where
	c.created_ts between d.install_ts and add_days(d.install_ts,180)
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.distinct_scenes_played=b.distinct_scenes_played
;

-- CREATE MEASURE FOR PLAYER MAXIMIZATION (SET MAXIMUM TO 5 AS UP TO 5 BADGES CAN BE EARNED PER SCENE)
alter table x_xsection add column maximizer decimal(12,5);
update x_xsection set maximizer=total_badges/distinct_scenes_played where total_badges is not null and distinct_scenes_played is not null;
update x_xsection set maximizer=5 where maximizer>5;

-- SET ALL ENTRIES THAT DID NOT HAPPEN WITHIN OBSERVATION PERIOD TO 0
update x_xsection set conversion_ts=null where days_between(to_date(conversion_ts),install_ts)>180;

alter table x_xsection add column conversion_lvl1 tinyint;
update x_xsection set conversion_lvl1=0;
update x_xsection set conversion_lvl1=1 where conversion_ts<=lvl1_complete_ts and conversion_ts is not null and lvl1_complete_ts is not null;


-- CREATE PANEL DATASET OVER 180 DAYS AFTER GAME DOWNLOAD

drop table x_panel;

-- CREATE GENERAL PANEL STRUCTURE
create table x_panel as
select
	*
from
(select
	distinct created_date
from
	x.app_logins
where
	created_date between '2016-12-15' and '2017-08-22')
full outer join
(select
	distinct unique_id
from
	x_xsection)
on 1=1
;

-- CREATE UNIQUE ID PER ROW FOR MERGING
alter table x_panel add column merge_id varchar(45);
update x_panel set merge_id=concat(created_date,unique_id);

-- GET RELEVANT INFO FROM CROSS-SECTIONAL DATASET

-- USERS' TREATMENT ASSIGNMENT
alter table x_panel add column treatment varchar(25);

merge into x_panel a using
(select
	unique_id,
	treatment
from
	x_xsection
group by
	1,2
) b
on (a.unique_id=b.unique_id)
when matched then update set a.treatment=b.treatment
;

-- INSTALL TS
alter table x_panel add column install_ts timestamp;

merge into x_panel a using
(select
	unique_id,
	install_ts
from
	x_xsection
group by
	1,2
) b
on (a.unique_id=b.unique_id)
when matched then update set a.install_ts=b.install_ts
;

-- WEEKDAY OF INSTALLATION
alter table x_panel add column weekday_install varchar(15);

merge into x_panel a using
(select
	unique_id,
	weekday_install
from
	x_xsection
group by
	1,2
) b
on (a.unique_id=b.unique_id)
when matched then update set a.weekday_install=b.weekday_install
;


-- COUNTRY
alter table x_panel add column country varchar(5);

merge into x_panel a using
(select
	unique_id,
	country
from
	x_xsection
group by
	1,2
) b
on (a.unique_id=b.unique_id)
when matched then update set a.country=b.country
;

-- MOBILE SOURCE DETAIL
alter table x_panel add column mobile_source_detail varchar(25);

merge into x_panel a using
(select
	unique_id,
	mobile_source_detail
from
	x_xsection
group by
	1,2
) b
on (a.unique_id=b.unique_id)
when matched then update set a.mobile_source_detail=b.mobile_source_detail
;

-- DEVICE MODEL
alter table x_panel add column device_model varchar(65);
--alter table x_panel drop column device_model;

merge into x_panel a using
(select
	unique_id,
	device_model
from
	x_xsection
group by
	1,2
) b
on (a.unique_id=b.unique_id)
when matched then update set a.device_model=b.device_model
;

-- INITIAL LANGUAGE
alter table x_panel add column initial_language varchar(5);

merge into x_panel a using
(select
	unique_id,
	initial_language
from
	x_xsection
group by
	1,2
) b
on (a.unique_id=b.unique_id)
when matched then update set a.initial_language=b.initial_language
;

-- LATEST LANGUAGE
alter table x_panel add column latest_language varchar(5);

merge into x_panel a using
(select
	unique_id,
	latest_language
from
	x_xsection
group by
	1,2
) b
on (a.unique_id=b.unique_id)
when matched then update set a.latest_language=b.latest_language
;

-- DEVICE RAM
alter table x_panel add column device_ram int;

merge into x_panel a using
(select
	unique_id,
	device_ram
from
	x_xsection
group by
	1,2
) b
on (a.unique_id=b.unique_id)
when matched then update set a.device_ram=b.device_ram
;

-- X RESOLUTION
alter table x_panel add column x_resolution int;

merge into x_panel a using
(select
	unique_id,
	x_resolution
from
	x_xsection
group by
	1,2
) b
on (a.unique_id=b.unique_id)
when matched then update set a.x_resolution=b.x_resolution
;

-- Y RESOLUTION
alter table x_panel add column y_resolution int;

merge into x_panel a using
(select
	unique_id,
	y_resolution
from
	x_xsection
group by
	1,2
) b
on (a.unique_id=b.unique_id)
when matched then update set a.y_resolution=b.y_resolution
;

-- DPI
alter table x_panel add column dpi int;

merge into x_panel a using
(select
	unique_id,
	dpi
from
	x_xsection
group by
	1,2
) b
on (a.unique_id=b.unique_id)
when matched then update set a.dpi=b.dpi
;

-- MAXIMIZER INDICATOR
alter table x_panel add column maximizer decimal(12,5);

merge into x_panel a using
(select
	unique_id,
	maximizer
from
	x_xsection
group by
	1,2
) b
on (a.unique_id=b.unique_id)
when matched then update set a.maximizer=b.maximizer
;

-- LEVEL ONE COMPLETION TIMESTAMP
alter table x_panel add column lvl1_complete_ts timestamp;

merge into x_panel a using
(select
	unique_id,
	lvl1_complete_ts
from
	x_xsection
group by
	1,2
) b
on (a.unique_id=b.unique_id)
when matched then update set a.lvl1_complete_ts=b.lvl1_complete_ts
;

-- LEVEL ONE OR HIGHER COMPLETION TIMESTAMP
alter table x_panel add column lvl1plus_complete_ts timestamp;

merge into x_panel a using
(select
	unique_id,
	lvl1plus_complete_ts
from
	x_xsection
group by
	1,2
) b
on (a.unique_id=b.unique_id)
when matched then update set a.lvl1plus_complete_ts=b.lvl1plus_complete_ts
;

-- GET USER BEHAVIOR DURING LEVEL 1 FROM XSECTION
alter table x_panel add column days_complete_lvl1 int;

merge into x_panel a using
(select
	unique_id,
	days_complete_lvl1
from
	x_xsection
) b
on (a.unique_id=b.unique_id)
when matched then update set a.days_complete_lvl1=b.days_complete_lvl1
;

alter table x_panel add column active_days_lvl1 int;

merge into x_panel a using
(select
	unique_id,
	active_days_lvl1
from
	x_xsection
) b
on (a.unique_id=b.unique_id)
when matched then update set a.active_days_lvl1=b.active_days_lvl1
;

alter table x_panel add column sessions_lvl1 int;

merge into x_panel a using
(select
	unique_id,
	sessions_lvl1
from
	x_xsection
) b
on (a.unique_id=b.unique_id)
when matched then update set a.sessions_lvl1=b.sessions_lvl1
;

alter table x_panel add column active_hours_lvl1 int;

merge into x_panel a using
(select
	unique_id,
	active_hours_lvl1
from
	x_xsection
) b
on (a.unique_id=b.unique_id)
when matched then update set a.active_hours_lvl1=b.active_hours_lvl1
;

alter table x_panel add column seconds_lvl1 int;

merge into x_panel a using
(select
	unique_id,
	seconds_lvl1
from
	x_xsection
) b
on (a.unique_id=b.unique_id)
when matched then update set a.seconds_lvl1=b.seconds_lvl1
;

alter table x_panel add column rounds_lvl1 int;

merge into x_panel a using
(select
	unique_id,
	rounds_lvl1
from
	x_xsection
) b
on (a.unique_id=b.unique_id)
when matched then update set a.rounds_lvl1=b.rounds_lvl1
;

alter table x_panel add column ads_lvl1 int;

merge into x_panel a using
(select
	unique_id,
	ads_lvl1
from
	x_xsection
) b
on (a.unique_id=b.unique_id)
when matched then update set a.ads_lvl1=b.ads_lvl1
;

alter table x_panel add column purchases_lvl1 int;

merge into x_panel a using
(select
	unique_id,
	purchases_lvl1
from
	x_xsection
) b
on (a.unique_id=b.unique_id)
when matched then update set a.purchases_lvl1=b.purchases_lvl1
;

alter table x_panel add column revenue_lvl1 decimal(12,5);

merge into x_panel a using
(select
	unique_id,
	revenue_lvl1
from
	x_xsection
) b
on (a.unique_id=b.unique_id)
when matched then update set a.revenue_lvl1=b.revenue_lvl1
;

alter table x_panel add column coins_bought_lvl1 int;

merge into x_panel a using
(select
	unique_id,
	coins_bought_lvl1
from
	x_xsection
) b
on (a.unique_id=b.unique_id)
when matched then update set a.coins_bought_lvl1=b.coins_bought_lvl1
;

alter table x_panel add column cash_bought_lvl1 int;

merge into x_panel a using
(select
	unique_id,
	cash_bought_lvl1
from
	x_xsection
) b
on (a.unique_id=b.unique_id)
when matched then update set a.cash_bought_lvl1=b.cash_bought_lvl1
;

alter table x_panel add column coins_spent_lvl1 int;

merge into x_panel a using
(select
	unique_id,
	coins_spent_lvl1
from
	x_xsection
) b
on (a.unique_id=b.unique_id)
when matched then update set a.coins_spent_lvl1=b.coins_spent_lvl1
;

alter table x_panel add column cash_spent_lvl1 int;

merge into x_panel a using
(select
	unique_id,
	cash_spent_lvl1
from
	x_xsection
) b
on (a.unique_id=b.unique_id)
when matched then update set a.cash_spent_lvl1=b.cash_spent_lvl1
;

alter table x_panel add column conversion_lvl1 tinyint;

merge into x_panel a using
(select
	unique_id,
	conversion_lvl1
from
	x_xsection
) b
on (a.unique_id=b.unique_id)
when matched then update set a.conversion_lvl1=b.conversion_lvl1
;


-- GET INFO FROM LOW LEVEL TRACKING DATA AND AGGREGATE BY i=USER/DEVICE and t=DAY IN UTC
-- SESSIONS
alter table x_panel add column sessions int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	count(unique_id) sessions
from
	x.app_logins
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.sessions=b.sessions
;

-- HOURS WITH ACTIVITY
alter table x_panel add column active_hours int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	count(distinct hour(created_ts)) active_hours
from
	x.app_logins
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.active_hours=b.active_hours
;

-- ROUNDS/SCENES PLAYED
alter table x_panel add column rounds_played int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	count(unique_id) rounds_played
from
	ppa_scene_end
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.rounds_played=b.rounds_played
;

-- SECONDS SPENT IN GAME
alter table x_panel add column seconds_in_game int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	round(sum(case when session_length>0 then session_length else 0 end),4) seconds_in_game
from
(select
	created_date,
	created_ts,
	unique_id,
	lead(last_session_length,1) over(partition by unique_id order by created_ts) session_length
from
	x.sessions
where
	created_date between '2016-12-15' and '2017-08-23')
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.seconds_in_game=b.seconds_in_game
;

-- ADS WATCHED
alter table x_panel add column ads int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	count(unique_id) ads
from		
	ppa_ads
where
	created_date between '2016-12-15' and '2017-08-22'
	and w_action='watch_ad'
group by		
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.ads=b.ads
;

-- PURCHASES
alter table x_panel add column purchases int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	count(unique_id) purchases
from
	bookings.bookings_x
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.purchases=b.purchases
;

-- REVENUE
alter table x_panel add column revenue decimal(12,5);

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	sum(bookings_usd) revenue
from
	bookings.bookings_x
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.revenue=b.revenue
;

-- PROMOTIONAL PURCHASES
alter table x_panel add column promo_purchases int;

merge into x_panel a using
(select		
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	count(unique_id) promo_purchases
from		
	bookings.bookings_x
where
	created_date between '2016-12-15' and '2017-08-22'
	and item like 'com.x.sb%'
group by		
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.promo_purchases=b.promo_purchases
;

-- PROMOTIONAL REVENUE
alter table x_panel add column promo_revenue decimal(12,5);

merge into x_panel a using
(select		
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	sum(bookings_usd) promo_revenue
from		
	bookings.bookings_x
where
	created_date between '2016-12-15' and '2017-08-22'
	and item like 'com.x.sb%'
group by		
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.promo_revenue=b.promo_revenue
;

-- SHOP PURCHASES
alter table x_panel add column shop_purchases int;
update x_panel set shop_purchases=coalesce(purchases,0)-coalesce(promo_purchases,0);

-- SHOP REVENUE
alter table x_panel add column shop_revenue decimal(12,5);
update x_panel set shop_revenue=coalesce(revenue,0)-coalesce(promo_revenue,0);

-- GET DAYS WITH PROMOTIONAL OFFERS IN THERE
alter table x_panel add column promo_day decimal(12,5);
update x_panel set promo_day=0;

update x_panel set promo_day=1 where created_date in (
'2016-12-16',
'2016-12-31',
'2017-01-13',
'2017-01-27',
'2017-02-10',
'2017-02-24',
'2017-03-10',
'2017-03-19',
'2017-03-31',
'2017-04-16',
'2017-04-28',
'2017-05-12',
'2017-05-26',
'2017-06-09',
'2017-06-23',
'2017-07-07',
'2017-07-21',
'2017-08-04',
'2017-08-18')
;

update x_panel set promo_day=2 where created_date in (
'2016-12-17',
'2017-01-01',
'2017-01-14',
'2017-01-28',
'2017-02-11',
'2017-02-25',
'2017-03-11',
'2017-03-20',
'2017-04-01',
'2017-04-17',
'2017-04-29',
'2017-05-13',
'2017-05-27',
'2017-06-10',
'2017-06-24',
'2017-07-08',
'2017-07-22',
'2017-08-05',
'2017-08-19')
;

update x_panel set promo_day=3 where created_date in (
'2016-12-18',
'2017-01-02',
'2017-01-15',
'2017-01-29',
'2017-02-12',
'2017-02-26',
'2017-03-12',
'2017-03-21',
'2017-04-02',
'2017-04-18',
'2017-04-30',
'2017-05-14',
'2017-05-28',
'2017-06-11',
'2017-06-25',
'2017-07-09',
'2017-07-23',
'2017-08-06',
'2017-08-20')
;

update x_panel set promo_day=4 where created_date in (
'2016-12-19',
'2017-01-16',
'2017-01-30',
'2017-02-13',
'2017-02-27',
'2017-03-13',
'2017-05-29',
'2017-06-26',
'2017-07-10',
'2017-07-24',
'2017-08-07',
'2017-08-21');

update x_panel set promo_day=5 where created_date in ('2017-02-14');

update x_panel set promo_day=-1 where created_date in (
'2016-12-15',
'2016-12-30',
'2017-01-12',
'2017-01-26',
'2017-02-09',
'2017-02-23',
'2017-03-09',
'2017-03-18',
'2017-03-30',
'2017-04-15',
'2017-04-27',
'2017-05-11',
'2017-05-25',
'2017-06-08',
'2017-06-22',
'2017-07-06',
'2017-07-20',
'2017-08-03',
'2017-08-17')
;

update x_panel set promo_day=-2 where created_date in (
'2016-12-14',
'2016-12-29',
'2017-01-11',
'2017-01-25',
'2017-02-08',
'2017-02-22',
'2017-03-08',
'2017-03-17',
'2017-03-29',
'2017-04-14',
'2017-04-26',
'2017-05-10',
'2017-05-24',
'2017-06-07',
'2017-06-21',
'2017-07-05',
'2017-07-19',
'2017-08-02',
'2017-08-16')
;

update x_panel set promo_day=-11 where created_date in (
'2017-01-03',
'2017-03-22',
'2017-04-03',
'2017-04-19',
'2017-05-01',
'2017-05-15',
'2017-06-12',
'2016-12-20',
'2017-01-17',
'2017-01-31',
'2017-02-28',
'2017-03-14',
'2017-05-30',
'2017-06-27',
'2017-07-11',
'2017-07-25',
'2017-08-08',
'2017-08-22',
'2017-02-15')
;

update x_panel set promo_day=-22 where created_date in (
'2017-01-04',
'2017-03-23',
'2017-04-04',
'2017-04-20',
'2017-05-02',
'2017-05-16',
'2017-06-13',
'2016-12-21',
'2017-01-18',
'2017-02-01',
'2017-03-01',
'2017-03-15',
'2017-05-31',
'2017-06-28',
'2017-07-12',
'2017-07-26',
'2017-08-09',
'2017-08-23',
'2017-02-16')
;


-- GET PER PLAYER PROMOTIONAL EXPOSURE IN THERE
alter table x_panel add column promo_offer tinyint;
update x_panel set promo_offer=1;

-- CONDITIONS FOR PROMO: LEVEL ONE COMPLETED; TREATMENT; NOT PURCHASED DURING PROMOTION
update x_panel set promo_offer=0 where created_date<coalesce(to_date(lvl1plus_complete_ts),'2017-12-31');
update x_panel set promo_offer=0 where created_date<add_days(to_date(install_ts),25) and treatment='after25days';
update x_panel set promo_offer=0 where created_date<add_days(to_date(install_ts),50) and treatment='after50days';
update x_panel set promo_offer=0 where treatment='no_promo';
update x_panel set promo_offer=0 where promo_day<=0;

-- get lagged promo revenue to calculate promo offer availability
alter table x_panel add column lag1_promo tinyint;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	case when (lag(promo_purchases,1) over(partition by unique_id order by created_date))>0 then 1 else 0 end lag1_promo
from
	x_panel
) b
on (a.merge_id=b.merge_id)
when matched then update set a.lag1_promo=b.lag1_promo
;

alter table x_panel add column lag2_promo tinyint;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	case when (lag(promo_purchases,2) over(partition by unique_id order by created_date))>0 then 1 else 0 end lag2_promo
from
	x_panel
) b
on (a.merge_id=b.merge_id)
when matched then update set a.lag2_promo=b.lag2_promo
;

alter table x_panel add column lag3_promo tinyint;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	case when (lag(promo_purchases,3) over(partition by unique_id order by created_date))>0 then 1 else 0 end lag3_promo
from
	x_panel
) b
on (a.merge_id=b.merge_id)
when matched then update set a.lag3_promo=b.lag3_promo
;

alter table x_panel add column lag4_promo tinyint;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	case when (lag(promo_purchases,4) over(partition by unique_id order by created_date))>0 then 1 else 0 end lag4_promo
from
	x_panel
) b
on (a.merge_id=b.merge_id)
when matched then update set a.lag4_promo=b.lag4_promo
;

alter table x_panel add column lag5_promo tinyint;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	case when (lag(promo_purchases,5) over(partition by unique_id order by created_date))>0 then 1 else 0 end lag5_promo
from
	x_panel
) b
on (a.merge_id=b.merge_id)
when matched then update set a.lag5_promo=b.lag5_promo
;

update x_panel set promo_offer=0 where lag1_promo>0;
update x_panel set promo_offer=0 where lag2_promo>0;
update x_panel set promo_offer=0 where lag3_promo>0;
update x_panel set promo_offer=0 where lag4_promo>0;
update x_panel set promo_offer=0 where lag5_promo>0;

alter table x_panel drop column lag1_promo;
alter table x_panel drop column lag2_promo;
alter table x_panel drop column lag3_promo;
alter table x_panel drop column lag4_promo;
alter table x_panel drop column lag5_promo;

-- GET COINS AND CASH BOUGHT IN THERE

-- COINS BOUGHT AT NORMAL PRICE
alter table x_panel add column coins_bought_normal int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	sum(quantity) coins_bought_normal
from
	bookings.bookings_x
where
	created_date between '2016-12-15' and '2017-08-22'
	and item='coins'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.coins_bought_normal=b.coins_bought_normal
;

-- CASH BOUGHT AT NORMAL PRICE
alter table x_panel add column cash_bought_normal int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	sum(quantity) cash_bought_normal
from
	bookings.bookings_x
where
	created_date between '2016-12-15' and '2017-08-22'
	and item='cash'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.cash_bought_normal=b.cash_bought_normal
;

-- COINS BOUGHT AT LOW PRICE
alter table x_panel add column coins_bought_low int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	(sum(case when item in ('com.x.bundle1a','com.x.sb1','com.x.sb2','com.x.sb8') then 1 else 0 end)*1000
	+sum(case when item='com.x.sb9' then 1 else 0 end)*1500
	+sum(case when item in ('com.x.sb3','com.x.sb10','com.x.sb11') then 1 else 0 end)*2000
	+sum(case when item in ('com.x.sb4','com.x.sb12') then 1 else 0 end)*2500
	+sum(case when item in ('com.x.sb5') then 1 else 0 end)*5000
	+sum(case when item in ('com.x.sb13') then 1 else 0 end)*8000) coins_bought_low
from
	bookings.bookings_x
where
	created_date between '2016-12-15' and '2017-08-22'
	and item like 'com.x.sb%'
group by
	1,2,3
) b
on (a.merge_id=b.merge_id)
when matched then update set a.coins_bought_low=b.coins_bought_low
;

-- CASH BOUGHT AT LOW PRICE
alter table x_panel add column cash_bought_low int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	(sum(case when item in ('com.x.sb9','com.x.bundle1a','com.x.sb1','com.x.sb2','com.x.sb8') then 1 else 0 end)*100
	+sum(case when item='com.x.sb10' then 1 else 0 end)*150
	+sum(case when item in ('com.x.sb3','com.x.sb11') then 1 else 0 end)*200
	+sum(case when item in ('com.x.sb4','com.x.sb12') then 1 else 0 end)*500
	+sum(case when item in ('com.x.sb5') then 1 else 0 end)*1000
	+sum(case when item in ('com.x.sb13') then 1 else 0 end)*1500) cash_bought_low
from
	bookings.bookings_x
where
	created_date between '2016-12-15' and '2017-08-22'
	and item like 'com.x.sb%'
group by
	1,2,3
) b
on (a.merge_id=b.merge_id)
when matched then update set a.cash_bought_low=b.cash_bought_low
;

-- GET COINS AND CASH SPENT IN THERE

-- COINS SPENT
alter table x_panel add column coins_spent int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	sum(buy_price) coins_spent
from
	x.purchases
where
	created_date between '2016-12-15' and '2017-08-22'
	and cash_coins='coins'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.coins_spent=b.coins_spent
;

-- CASH SPENT
alter table x_panel add column cash_spent int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	sum(buy_price) cash_spent
from
	x.purchases
where
	created_date between '2016-12-15' and '2017-08-22'
	and cash_coins='cash'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.cash_spent=b.cash_spent
;

-- GET COINS AND CASH MIN, MAX AND AVERAGE STOCK

-- AVG COIN STOCK
alter table x_panel add column avg_coins int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	round(avg(coin_count_pit),0) avg_coins
from
	x.sessions
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.avg_coins=b.avg_coins
;

-- AVG CASH STOCK
alter table x_panel add column avg_cash int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	round(avg(cash_count_pit),0) avg_cash
from
	x.sessions
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.avg_cash=b.avg_cash
;

-- MEDIAN COIN STOCK
alter table x_panel add column median_coins int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	median(coin_count_pit) median_coins
from
	x.sessions
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.median_coins=b.median_coins
;

-- MEDIAN CASH STOCK
alter table x_panel add column median_cash int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	median(cash_count_pit) median_cash
from
	x.sessions
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.median_cash=b.median_cash
;

-- GET SKILL RELATED VARIABLES IN THERE
-- TIME TO FINISH SCENE
alter table x_panel add column time_finish_scene decimal(12,5);

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	sum(time_to_finish_scene) time_finish_scene
from
	x.scene_end
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.time_finish_scene=b.time_finish_scene
;

-- SCENE MASTERY
alter table x_panel add column scene_mastery decimal(20,5);

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	avg(scene_mastery_pit) scene_mastery
from
	x.scene_end
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.scene_mastery=b.scene_mastery
;

-- SCORE EARNED
alter table x_panel add column score_earned bigint;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	sum(score_earned) score_earned
from
	x.scene_end
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.score_earned=b.score_earned
;

-- BADGES EARNED
alter table x_panel add column badges_earned int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	sum(badges_sceneend-badges_scenestart) badges_earned
from
	x.scene_end
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.badges_earned=b.badges_earned
;

-- MIN SCENE ID OF DAY
alter table x_panel add column min_scene varchar(12);

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	min(scene_id) min_scene
from
	x.scene_end
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.min_scene=b.min_scene
;

-- MAX SCENE ID OF DAY
alter table x_panel add column max_scene varchar(12);

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	max(scene_id) max_scene
from
	x.scene_end
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.max_scene=b.max_scene
;

-- SCENES COMPLETED
alter table x_panel add column scenes_completed int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	case
	when to_number(left(right(max(scene_id),5),2))=to_number(left(right(min(scene_id),5),2)) then to_number(right(max(scene_id),2))-to_number(right(min(scene_id),2))
	else (to_number(left(right(max(scene_id),5),2))-to_number(left(right(min(scene_id),5),2))-1)*6 + (6-to_number(right(min(scene_id),2))+1) + (to_number(right(max(scene_id),2))-1)
	end scenes_completed
from
	x.scene_end
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.scenes_completed=b.scenes_completed
;

-- DISTINCT SCENES PLAYED
alter table x_panel add column distinct_scenes_played int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	count(distinct scene_id) distinct_scenes_played
from
	x.scene_end
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.distinct_scenes_played=b.distinct_scenes_played
;

-- GET ACTIVITY IN OTHER GAMES IN THERE
-- SESSIONS y4
alter table x_panel add column y4_sessions int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	count(unique_id) sessions
from
	y4.app_logins
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.y4_sessions=b.sessions
;

-- HOURS WITH ACTIVITY y4
alter table x_panel add column y4_active_hours int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	count(distinct hour(created_ts)) active_hours
from
	y4.app_logins
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.y4_active_hours=b.active_hours
;

-- SESSIONS y3
alter table x_panel add column y3_sessions int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	count(unique_id) sessions
from
	y3.app_logins
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.y3_sessions=b.sessions
;

-- HOURS WITH ACTIVITY y3
alter table x_panel add column y3_active_hours int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	count(distinct hour(created_ts)) active_hours
from
	y3.app_logins
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.y3_active_hours=b.active_hours
;

-- SESSIONS y1
alter table x_panel add column y1_sessions int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	count(unique_id) sessions
from
	y1.app_logins
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.y1_sessions=b.sessions
;

-- HOURS WITH ACTIVITY y1
alter table x_panel add column y1_active_hours int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	count(distinct hour(created_ts)) active_hours
from
	y1.app_logins
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.y1_active_hours=b.active_hours
;


-- GET SPENDING IN OTHER GAMES IN THERE
-- REVENUE/SPENDING y4
alter table x_panel add column y4_revenue decimal(12,5);

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	sum(bookings_usd) revenue
from
	bookings.bookings_y4
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.y4_revenue=b.revenue
;

-- PURCHASES y4
alter table x_panel add column y4_purchases int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	count(unique_id) purchases
from
	bookings.bookings_y4
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.y4_purchases=b.purchases
;

-- REVENUE/SPENDING y3
alter table x_panel add column y3_revenue decimal(12,5);

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	sum(bookings_usd) revenue
from
	bookings.bookings_y3
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.y3_revenue=b.revenue
;

-- PURCHASES y3
alter table x_panel add column y3_purchases int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	count(unique_id) purchases
from
	bookings.bookings_y3
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.y3_purchases=b.purchases
;

-- REVENUE/SPENDING y1
alter table x_panel add column y1_revenue decimal(12,5);

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	sum(bookings_usd) revenue
from
	bookings.bookings_y1
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.y1_revenue=b.revenue
;

-- PURCHASES y1
alter table x_panel add column y1_purchases int;

merge into x_panel a using
(select
	created_date,
	unique_id,
	concat(created_date,unique_id) merge_id,
	count(unique_id) purchases
from
	bookings.bookings_y1
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1,2
) b
on (a.merge_id=b.merge_id)
when matched then update set a.y1_purchases=b.purchases
;

-- ADD PORTFOLIO AGGREGATE VARS
alter table x_panel add column portfolio_sessions int;
update x_panel set portfolio_sessions=coalesce(y1_sessions,0)+coalesce(y4_sessions,0)+coalesce(y3_sessions,0);

alter table x_panel add column portfolio_active_hours int;
update x_panel set portfolio_active_hours=coalesce(y1_active_hours,0)+coalesce(y4_active_hours,0)+coalesce(y3_active_hours,0);

alter table x_panel add column portfolio_purchases int;
update x_panel set portfolio_purchases=coalesce(y1_purchases,0)+coalesce(y4_purchases,0)+coalesce(y3_purchases,0);

alter table x_panel add column portfolio_revenue decimal(12,5);
update x_panel set portfolio_revenue=coalesce(y1_revenue,0)+coalesce(y4_revenue,0)+coalesce(y3_revenue,0);

alter table x_panel drop column y4_revenue;
alter table x_panel drop column y4_sessions;
alter table x_panel drop column y4_purchases;
alter table x_panel drop column y4_active_hours;
alter table x_panel drop column y1_revenue;
alter table x_panel drop column y1_sessions;
alter table x_panel drop column y1_purchases;
alter table x_panel drop column y1_active_hours;
alter table x_panel drop column y3_revenue;
alter table x_panel drop column y3_sessions;
alter table x_panel drop column y3_purchases;
alter table x_panel drop column y3_active_hours;

-- ADD GENERIC TIME VARIABLE FOR PANEL (day of game download is t=1)
alter table x_panel add column t_day int;
update x_panel set t_day=(days_between(created_date,to_date(install_ts))+1);

-- ADD DAY OF WEEK
alter table x_panel add column day_of_week varchar(15);
update x_panel set day_of_week=TO_CHAR(created_date,'DAY');


-- ADD LAST ACTIVE DAY IN OBSERVATION PERIOD
alter table x_panel add column last_active date;

merge into x_panel a using
(select
	unique_id,
	max(last_active) last_active
from
	((select unique_id, max(created_date) last_active from x.app_logins where created_date between '2016-12-15' and '2017-08-22' group by 1)
	union
	(select unique_id, max(created_date) last_active from x.sessions where created_date between '2016-12-15' and '2017-08-22' group by 1)
	union
	(select unique_id, max(created_date) last_active from x.scene_end where created_date between '2016-12-15' and '2017-08-22' group by 1))
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.last_active=b.last_active
;

-- ADD FIRST CONVERSION DAY
alter table x_panel add column first_purchase date;

merge into x_panel a using
(select
	unique_id,
	min(created_date) first_purchase
from
	bookings.bookings_x
where
	created_date between '2016-12-15' and '2017-08-22'
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.first_purchase=b.first_purchase
;

alter table x_panel add column conversion tinyint;
update x_panel set conversion=0;
update x_panel set conversion=1 where first_purchase=created_date;


-- GET BEHAVIORAL AGGREGATES FROM PANEL INTO XSECTION

-- SESSIONS
alter table x_xsection add column sessions int;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(sessions,0)) sessions
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.sessions=b.sessions
;

-- SECONDS IN GAME
alter table x_xsection add column seconds_in_game bigint;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(seconds_in_game,0)) seconds_in_game
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.seconds_in_game=b.seconds_in_game
;

-- ROUNDS
alter table x_xsection add column rounds_played int;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(rounds_played,0)) rounds_played
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.rounds_played=b.rounds_played
;

-- ACTIVE HOURS
alter table x_xsection add column active_hours int;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(active_hours,0)) active_hours
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.active_hours=b.active_hours
;

-- ACTIVE DAYS
alter table x_xsection add column active_days int;

merge into x_xsection a using
(select		
	unique_id,	
	count(distinct t_day) active_days
from		
	x_panel
where
	sessions>=1
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.active_days=b.active_days
;

-- ADS WATCHED
alter table x_xsection add column ads int;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(ads,0)) ads
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.ads=b.ads
;

-- REVENUE
alter table x_xsection add column revenue decimal(12,5);

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(revenue,0)) revenue
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.revenue=b.revenue
;

-- PURCHASES
alter table x_xsection add column purchases int;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(purchases,0)) purchases
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.purchases=b.purchases
;

-- PROMO REVENUE
alter table x_xsection add column promo_revenue decimal(12,5);

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(promo_revenue,0)) promo_revenue
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.promo_revenue=b.promo_revenue
;

-- PROMO PURCHASES
alter table x_xsection add column promo_purchases int;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(promo_purchases,0)) promo_purchases
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.promo_purchases=b.promo_purchases
;

-- SHOP REVENUE
alter table x_xsection add column shop_revenue decimal(12,5);
update x_xsection set shop_revenue=coalesce(revenue,0)-coalesce(promo_revenue,0);

-- SHOP PURCHASES
alter table x_xsection add column shop_purchases int;
update x_xsection set shop_purchases=coalesce(purchases,0)-coalesce(promo_purchases,0);

-- CALENDAR DAYS TO FIRST PRICE PROMOTION
alter table x_xsection add column days_first_promo_offer int;

merge into x_xsection a using
(select		
	unique_id,	
	days_between(min(created_date),to_date(min(install_ts))) days_first_promo_offer
from		
	x_panel
where
	promo_offer=1
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.days_first_promo_offer=b.days_first_promo_offer
;

-- ALSO GET THEM INTO THE PANEL

-- DAYS TO FIRST PROMO OFFER
alter table x_panel add column days_first_promo_offer int;

merge into x_panel a using
(select
	unique_id,
	days_first_promo_offer
from
	x_xsection
group by
	1,2
) b
on (a.unique_id=b.unique_id)
when matched then update set a.days_first_promo_offer=b.days_first_promo_offer
;

-- CALENDAR DAYS TO FIRST PURCHASE
alter table x_xsection add column days_first_purchase int;

merge into x_xsection a using
(select		
	unique_id,	
	days_between(min(created_date),to_date(min(install_ts))) days_first_purchase
from		
	x_panel
where
	purchases>=1
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.days_first_purchase=b.days_first_purchase
;

-- CALENDAR DAYS TO FIRST PROMO PURCHASE
alter table x_xsection add column days_first_promo_purchase int;

merge into x_xsection a using
(select		
	unique_id,	
	days_between(min(created_date),to_date(min(install_ts))) days_first_promo_purchase
from		
	x_panel
where
	promo_purchases>=1
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.days_first_promo_purchase=b.days_first_promo_purchase
;

-- CALENDAR DAYS TO FIRST SHOP PURCHASE
alter table x_xsection add column days_first_shop_purchase int;

merge into x_xsection a using
(select		
	unique_id,	
	days_between(min(created_date),to_date(min(install_ts))) days_first_shop_purchase
from		
	x_panel
where
	shop_purchases>=1
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.days_first_shop_purchase=b.days_first_shop_purchase
;

-- TIME TO FINISH SCENE
alter table x_xsection add column time_finish_scene decimal(12,5);

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(time_finish_scene,0)) time_finish_scene
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.time_finish_scene=b.time_finish_scene
;

-- SCENE MASTERY
alter table x_xsection add column scene_mastery decimal(20,5);

merge into x_xsection a using
(select		
	unique_id,	
	avg(scene_mastery) scene_mastery
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.scene_mastery=b.scene_mastery
;

-- SCORE EARNED
alter table x_xsection add column score_earned bigint;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(score_earned,0)) score_earned
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.score_earned=b.score_earned
;

-- BADGES EARNED
alter table x_xsection add column badges_earned bigint;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(badges_earned,0)) badges_earned
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.badges_earned=b.badges_earned
;

-- SCENES COMPLETED
alter table x_xsection add column scenes_completed bigint;

merge into x_xsection a using
(select		
	unique_id,	
	sum(scenes_completed) scenes_completed
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.scenes_completed=b.scenes_completed
;

-- PORTFOLIO BEHAVIOR DURING TREATMENT AND OBSERVATION PERIOD
alter table x_xsection add column portfolio_revenue decimal(12,5);

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(portfolio_revenue,0)) portfolio_revenue
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.portfolio_revenue=b.portfolio_revenue
;

alter table x_xsection add column portfolio_purchases int;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(portfolio_purchases,0)) portfolio_purchases
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.portfolio_purchases=b.portfolio_purchases
;

alter table x_xsection add column portfolio_active_hours int;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(portfolio_active_hours,0)) portfolio_active_hours
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.portfolio_active_hours=b.portfolio_active_hours
;

alter table x_xsection add column portfolio_sessions int;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(portfolio_sessions,0)) portfolio_sessions
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.portfolio_sessions=b.portfolio_sessions
;

alter table x_xsection add column coins_bought_normal int;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(coins_bought_normal,0)) coins_bought_normal
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.coins_bought_normal=b.coins_bought_normal
;

alter table x_xsection add column cash_bought_normal int;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(cash_bought_normal,0)) cash_bought_normal
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.cash_bought_normal=b.cash_bought_normal
;

alter table x_xsection add column coins_bought_low int;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(coins_bought_low,0)) coins_bought_low
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.coins_bought_low=b.coins_bought_low
;

alter table x_xsection add column cash_bought_low int;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(cash_bought_low,0)) cash_bought_low
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.cash_bought_low=b.cash_bought_low
;

alter table x_xsection add column coins_spent int;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(coins_spent,0)) coins_spent
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.coins_spent=b.coins_spent
;

alter table x_xsection add column cash_spent int;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(cash_spent,0)) cash_spent
from		
	x_panel
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.cash_spent=b.cash_spent
;

alter table x_xsection add column conversion tinyint;
update x_xsection set conversion=0;
update x_xsection set conversion=1 where coalesce(revenue,0)>0;

-- GET SOME ADDITIONAL INFO FROM USER BASE
alter table x_xsection add column requests_sent int;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(d180_requests_sent,0)) requests_sent
from		
	x.user_base
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.requests_sent=b.requests_sent
;

alter table x_xsection add column ad_revenue decimal(12,5);

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(d180_ad_revenue,0)) ad_revenue
from		
	x.user_base
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.ad_revenue=b.ad_revenue
;

alter table x_xsection add column interstitial_ads int;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(d180_interstitial_impressions,0)) interstitial_ads
from		
	x.user_base
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.interstitial_ads=b.interstitial_ads
;

alter table x_xsection add column video_ads int;

merge into x_xsection a using
(select		
	unique_id,	
	sum(coalesce(d180_video_impressions,0)) video_ads
from		
	x.user_base
group by
	1
) b
on (a.unique_id=b.unique_id)
when matched then update set a.video_ads=b.video_ads
;
