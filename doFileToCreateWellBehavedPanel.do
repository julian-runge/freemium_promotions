clear all

// load separate csvs and save as dta
cap cd "/Data"

insheet using "x_panel_treated.csv", delimiter(";") names clear
save "x_panel_treated.dta", replace

// created id for each user
egen id=group(hashed_unique_id)

// set to panel dataset
xtset id t_day

// transform treatment var from string to numeric
egen treatment2=group(treatment)

// transform weekday of installation from string to numeric
egen weekday_install2=group(weekday_install)
egen day_of_week2=group(day_of_week)

// transform background variables from string to numeric
egen country2=group(country)
egen device_model2=group(device_model)
egen mobile_source_detail2=group(mobile_source_detail)

// replace missing values by zero for behavioral outcomes
replace sessions=0 if missing(sessions)
replace seconds_in_game=0 if missing(seconds_in_game)
replace rounds_played=0 if missing(rounds_played)
replace active_hours=0 if missing(active_hours)
replace conversion=0 if missing(conversion)
replace revenue=0 if missing(revenue)
replace purchases=0 if missing(purchases)
replace promo_revenue=0 if missing(promo_revenue)
replace promo_purchases=0 if missing(promo_purchases)
replace shop_revenue=0 if missing(shop_revenue)
replace shop_purchases=0 if missing(shop_purchases)
replace promo_day=0 if missing(promo_day)
replace promo_offer=0 if missing(promo_offer)
replace portfolio_revenue=0 if missing(portfolio_revenue)
replace portfolio_purchases=0 if missing(portfolio_purchases)
replace portfolio_active_hours=0 if missing(portfolio_active_hours)
replace portfolio_sessions=0 if missing(portfolio_sessions)
replace coins_bought_normal=0 if missing(coins_bought_normal)
replace cash_bought_normal=0 if missing(cash_bought_normal)
replace coins_bought_low=0 if missing(coins_bought_low)
replace cash_bought_low=0 if missing(cash_bought_low)
replace coins_spent=0 if missing(coins_spent)
replace cash_spent=0 if missing(cash_spent)
replace days_complete_lvl1=0 if missing(days_complete_lvl1)
replace active_days_lvl1=0 if missing(active_days_lvl1)
replace sessions_lvl1=0 if missing(sessions_lvl1)
replace active_hours_lvl1=0 if missing(active_hours_lvl1)
replace seconds_lvl1=0 if missing(seconds_lvl1)
replace rounds_lvl1=0 if missing(rounds_lvl1)
replace ads_lvl1=0 if missing(ads_lvl1)
replace ads=0 if missing(ads)
replace purchases_lvl1=0 if missing(purchases_lvl1)
replace revenue_lvl1=0 if missing(revenue_lvl1)
replace score_earned=0 if missing(score_earned)
replace badges_earned=0 if missing(badges_earned)
replace scenes_completed=0 if missing(scenes_completed)

// replace missings of tech variables with their medians
replace device_ram=1705 if missing(device_ram)
replace x_resolution=1280 if missing(x_resolution)
replace y_resolution=1280 if missing(y_resolution)
replace dpi=320 if missing(dpi)

// replace missings of categorical vars with NA
replace country="NA" if missing(country)
replace mobile_source_detail="NA" if missing(mobile_source_detail)
replace device_model="NA" if missing(device_model)
replace initial_language="NA" if missing(initial_language)
replace latest_language="NA" if missing(latest_language)
replace last_active="NA" if missing(last_active)

// log key outcomes variables
//gen log_sessions=log(sessions+1)
//gen log_rounds_played=log(rounds_played+1)

// generate additional helpful/needed variables
gen hours_in_game=seconds_in_game/3600
gen coins_bought=coins_bought_low+coins_bought_normal
gen cash_bought=cash_bought_low+cash_bought_normal

egen panel_conversion = max(conversion), by(id)

gen high_value=0
replace high_value=1 if device_ram>1784

egen purchases_per_user = sum(purchases), by(id)

gen purchase_binary=0
replace purchase_binary=1 if purchases>0

// generate cumulative purchases
//bysort id (t_day) : gen purchases_cumul=sum(purchases)

// take log of t
gen log_t = log(t_day+1)

// take log of days at level 1
gen log_days_level1 = log(days_complete_lvl1+1)

// generate dummies for treatment conditions
gen t0=0
replace t0=1 if treatment=="after0days"

gen t25=0
replace t25=1 if treatment=="after25days"

gen t50=0
replace t50=1 if treatment=="after50days"

gen c=0
replace c=1 if treatment=="no_promo"

// generate pooled treatment indicator
gen t=0
replace t=1 if (t0==1 | t25==1 | t50==1)

// generate dummies for key countries
gen us=0
replace us=1 if country=="US"

gen uk=0
replace uk=1 if country=="GB"

gen de=0
replace de=1 if country=="DE"

gen fr=0
replace fr=1 if country=="FR"

// generate more descriptive promo day variable
gen promo_day2 = "NA"
replace promo_day2 = "day before promo" if promo_day==-1
replace promo_day2 = "two days before promo" if promo_day==-2
replace promo_day2 = "day after promo" if promo_day==-11
replace promo_day2 = "two days after promo" if promo_day==-22
replace promo_day2 = "first promo day" if promo_day==1
replace promo_day2 = "second promo day" if promo_day==2
replace promo_day2 = "third promo day" if promo_day==3
replace promo_day2 = "fourth promo day" if promo_day==4
replace promo_day2 = "fifth promo day" if promo_day==5
replace promo_day2 = "all other days" if promo_day==0

// generate descriptive variable labels
label variable t_day "Observation day t, with t between 1 and 181"
label variable hashed_unique_id "Hashed unique identifier"
label variable id "User ID"
label variable treatment "Treatment the user was assigned to"
label variable install_ts "Timestamp of game download"
label variable promo_day "Day of and around promotion (promotions ran up to 5 days, users can only buy once during a promotion)"
label variable promo_day2 "Day of and around promotion with descriptive labels"
label variable promo_offer "Indicator if promotional offer available to user that day"
label variable country "Country where game was downloaded"
label variable mobile_source_detail "Detailed descriptor of user source"
label variable device_model "Model of Android device used to play game"
label variable latest_language "Latest language"
label variable initial_language "Language on device when product was first used"
label variable device_ram "Memory of device used to play game (higher RAM = higher device price on average)"
label variable x_resolution "Device resolution in x dimension"
label variable y_resolution "Device resolution in y dimension"
label variable dpi "DPI (dots per inch) of device, indicator of display quality"
label variable lvl1_complete_ts "Timestamp when level one was completed"
label variable lvl1plus_complete_ts "Timestamp when level one or higher was completed"
label variable sessions "Number of times the app was opened that day"
label variable active_hours "Number of full hours during which app was opened that day"
label variable rounds_played "Number of game rounds played during that day"
label variable seconds_in_game "Time spent in app in seconds"
label variable conversion "Indicator of first purchase incidence"
label variable purchases "Number of purchases made that day"
label variable revenue "Revenue generated by a user that day"
label variable promo_revenue "Revenue generated by buying promotional offers"
label variable promo_purchases "Number of promotional purchases made by a user that day"
label variable shop_revenue "Revenue generated in the normal game shop at non-promotional price"
label variable shop_purchases "Purchases made in the normal game shop"
label variable avg_coins "Average stock of coins recorded on day t"
label variable median_coins "Median stock of coins recorded on day t"
label variable avg_cash "Average stock of cash recorded on day t"
label variable median_cash "Median stock of cash recorded on day t"
label variable last_active "Date when user was last active in game"
label variable first_purchase "Date of first purchase in game"
label variable portfolio_sessions "Number of sessions in other portfolio games that day"
label variable portfolio_active_hours "Sessions registered before completion of level one"
label variable portfolio_revenue "Revenue generated in other portfolio games by a user that day"
label variable portfolio_purchases "Purchases made in other portfolio games"
label variable treatment2 "Numeric treatment assignment indicator"
label variable t0 "Indicator for immediate promotions treatment (after 0 days)"
label variable t25 "Indicator for intermediate promotions treatment (after 25 days)"
label variable t50 "Indicator for late promotions treatment (after 50 days)"
label variable c "Indicator for no promotions treatment (not until day 180 of lifetime)"
label variable us "Indicator if game downloaded in the US"
label variable uk "Indicator if game downloaded in the UK"
label variable de "Indicator if game downloaded in Germany"
label variable fr "Indicator if game downloaded in France"
label variable hours_in_game "Hours spent in game on day t"
label variable coins_bought "Coins bought overall on day t"
label variable cash_bought "Cash bought overall on day t"
label variable coins_bought_normal "Coins bought at normal shop price on day t"
label variable cash_bought_normal "Cash bought at normal shop price on day t"
label variable coins_bought_low "Coins bought at promotional price on day t"
label variable cash_bought_low "Cash bought at promotional price on day t"
label variable coins_spent "Coins spent overall on day t"
label variable cash_spent "Cash spent overall on day t"
label variable ads "Video ads watched by user on day t"
label variable days_complete_lvl1 "Calendar days needed to complete level one"
label variable active_days_lvl1 "Days with activity during level one"
label variable sessions_lvl1 "Sessions played during level one"
label variable active_hours_lvl1 "Hours with activity during level one"
label variable seconds_lvl1 "Seconds played during level one"
label variable rounds_lvl1 "Rounds played during level one"
label variable ads_lvl1 "Ads watched during level one"
label variable purchases_lvl1 "Purchases made during level one"
label variable revenue_lvl1 "Revenue generated during level one"
label variable created_date "Calendar date of day t"
label variable day_of_week "Day of week of day t"
label variable days_first_promo_offer "Calendar days to first promotional offer"
label variable weekday_install "Day of week of game download"
label variable coins_bought_lvl1 "Coins bought during level one"
label variable cash_bought_lvl1 "Cash bought during level one"
label variable coins_spent_lvl1 "Coins spent during level one"
label variable cash_spent_lvl1 "Cahs spent during level one"
label variable conversion_lvl1 "Indicator if user made a purchase during level one"
label variable time_finish_scene "Time used to finish scenes on day t"
label variable score_earned "Total score earned on day t"
label variable badges_earned "Total badges earned on day t"
label variable scene_mastery "Average mastery displayed in playing scenes on day t"
label variable scenes_completed "Total number of scenes that were successfully completed on day t"
label variable high_value "Exogenous indicator of high WTP users, derived from median split on device memory"
label variable panel_conversion "Indicator if user made a purchase"
label variable purchases_per_user "Overall purchases per user"
label variable purchase_binary "Indicator if user made a purchase on day t or not"
label variable maximizer "Average number of badges collected per scene - indicator if a user maximizes versus satifices"
label variable min_scene "Minimum scene played on day t"
label variable max_scene "Maximum scene played on day t"
label variable distinct_scenes_played "Distinct scenes played on day t"
label variable t "Indicator if user was treated with promotions versus not"

// save dataset as dta file
save "x_panel_treated_cleaned.dta", replace

use "x_panel_treated_cleaned.dta", clear

// drop all non-crucial variables
keep id t_day created_date t c t0 t25 t50 purchase_binary coins_bought cash_bought coins_bought_normal ///
	cash_bought_normal coins_bought_low cash_bought_low coins_spent cash_spent ///
	revenue purchases shop_revenue shop_purchases promo_revenue promo_purchases ///
	promo_offer promo_day promo_day2 sessions rounds_played seconds_in_game
	
// save reduced dataset as dta file
save "x_panel_treated_cleaned_reduced.dta", replace
