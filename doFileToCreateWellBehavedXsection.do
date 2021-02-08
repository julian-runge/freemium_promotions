clear all

// load data
insheet using "/Data/x_xsection.csv", delimiter(";") names clear

// created id for each user
egen id=group(hashed_unique_id)

// transform treatment var from string to numeric
egen treatment2=group(treatment)

// transform weekday of installation from string to numeric
egen weekday_install2=group(weekday_install)

// replace missing values by zero for numeric behavioral outcomes
// do not impute missings for lvl1 variables as these are missing for everyone
// who did not complete level 1
replace portfolio_d30devrev=0 if missing(portfolio_d30devrev)
replace portfolio_d30rev=0 if missing(portfolio_d30rev)
replace portfolio_d30rounds=0 if missing(portfolio_d30rounds)

replace sessions_lvl1=0 if missing(sessions_lvl1) & lvl1_complete==1
replace seconds_lvl1=0 if missing(seconds_lvl1) & lvl1_complete==1
replace rounds_lvl1=0 if missing(rounds_lvl1) & lvl1_complete==1
replace active_hours_lvl1=0 if missing(active_hours_lvl1) & lvl1_complete==1
replace active_days_lvl1=0 if missing(active_days_lvl1) & lvl1_complete==1
replace conversion_lvl1=0 if missing(conversion_lvl1) & lvl1_complete==1
replace revenue_lvl1=0 if missing(revenue_lvl1) & lvl1_complete==1
replace purchases_lvl1=0 if missing(purchases_lvl1) & lvl1_complete==1
replace ads_lvl1=0 if missing(ads_lvl1) & lvl1_complete==1
replace coins_bought_lvl1=0 if missing(coins_bought_lvl1) & lvl1_complete==1
replace cash_bought_lvl1=0 if missing(cash_bought_lvl1) & lvl1_complete==1
replace coins_spent_lvl1=0 if missing(coins_spent_lvl1) & lvl1_complete==1
replace cash_spent_lvl1=0 if missing(cash_spent_lvl1) & lvl1_complete==1

replace sessions=0 if missing(sessions)
replace seconds_in_game=0 if missing(seconds_in_game)
replace rounds_played=0 if missing(rounds_played)
replace active_hours=0 if missing(active_hours)
replace active_days=0 if missing(active_days)
replace conversion=0 if missing(conversion)
replace revenue=0 if missing(revenue)
replace purchases=0 if missing(purchases)
replace promo_revenue=0 if missing(promo_revenue)
replace promo_purchases=0 if missing(promo_purchases)
replace shop_revenue=0 if missing(shop_revenue)
replace shop_purchases=0 if missing(shop_purchases)
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
replace ads=0 if missing(ads)
replace video_ads=0 if missing(video_ads)
replace interstitial_ads=0 if missing(interstitial_ads)
replace ad_revenue=0 if missing(ad_revenue)
replace requests_sent=0 if missing(requests_sent)
replace score_earned=0 if missing(score_earned)
replace badges_earned=0 if missing(badges_earned)
replace scenes_completed=0 if missing(scenes_completed)

replace lvl1_complete=0 if missing(lvl1_complete)

// replace missings of tech variables with their medians
replace device_ram=1705 if missing(device_ram)
replace x_resolution=1280 if missing(x_resolution)
replace y_resolution=1280 if missing(y_resolution)
replace dpi=320 if missing(dpi)

// replace missings of categorical vars with NA
replace country="NA" if missing(country)
replace mobile_source="NA" if missing(country)
replace mobile_source_detail="NA" if missing(country)
replace device="NA" if missing(country)
replace device_model="NA" if missing(country)
replace initial_language="NA" if missing(country)
replace latest_language="NA" if missing(country)

// replace missings of stock variables with their median
replace min_coins=4521 if missing(min_coins)
replace max_coins=5000 if missing(max_coins)
replace avg_coins=4719 if missing(avg_coins)
replace median_coins=4678 if missing(median_coins)
replace min_cash=93 if missing(min_cash)
replace max_cash=98 if missing(max_cash)
replace avg_cash=98 if missing(avg_cash)
replace median_cash=98 if missing(median_cash)

// generate some additional helpful variables
gen coins_bought=coins_bought_low+coins_bought_normal
gen cash_bought=cash_bought_low+cash_bought_normal
gen hours_in_game=seconds_in_game/(3600)
gen days_in_game=seconds_in_game/(3600*24)

gen overall_revenue = revenue + ad_revenue

// generate dummy for highly engaged users using median splits on pre-treatment indicator of consumption pace
summarize days_complete_lvl1 if lvl1_complete==1, detail //median is 2
gen high_engage=0
replace high_engage=1 if days_complete_lvl1<=2

// generate dummy for high value users based on device memory (strongest correlate of spending)
summarize device_ram if lvl1_complete==1, detail //median is 1784
gen high_value=0
replace high_value=1 if device_ram>1784

// look at seconds in game using days in game (checking if it's reasonable)
summarize days_in_game, detail
//--> it's not reasonable
//--> impute maximum for seconds_in_game as active_days*16*3600
//(assuming that, on each active day, players would not play more than 16 hours)
replace seconds_in_game=active_days*16*3600 if seconds_in_game>=active_days*16*3600
replace days_in_game=seconds_in_game/(3600*24)
replace hours_in_game=seconds_in_game/3600

replace seconds_lvl1=active_days_lvl1*16*3600 if seconds_lvl1>=active_days_lvl1*16*3600
gen hours_lvl1=seconds_lvl1/3600

// log key outcomes variables
gen log_revenue=log(revenue+1)
gen log_shop_revenue=log(shop_revenue+1)
gen log_promo_revenue=log(promo_revenue+1)
gen log_active_hours=log(active_hours+1)
gen log_rounds_played=log(rounds_played+1)
gen log_sessions=log(sessions+1)
gen log_seconds_in_game=log(seconds_in_game+1)
gen log_hours_in_game=log(hours_in_game+1)

// generate dummies for treatment conditions
gen t0=0
replace t0=1 if treatment=="after0days"

gen t25=0
replace t25=1 if treatment=="after25days"

gen t50=0
replace t50=1 if treatment=="after50days"

gen c=0
replace c=1 if treatment=="no_promo"

// generate dummies for key countries
gen us=0
replace us=1 if country=="US"

gen uk=0
replace uk=1 if country=="GB"

gen de=0
replace de=1 if country=="DE"

gen fr=0
replace fr=1 if country=="FR"

// generate descriptive variable labels
label variable hashed_unique_id "Hashed unique identifier"
label variable treatment "Treatment the user was assigned to"
label variable id "Device ID / User ID"
label variable weekday_install "Day of week of game download"
label variable weekday_install2 "Day of week of game download (numeric grouping)"
label variable install_date "Start date of product use / date when game was installed"
label variable install_ts "Timestamp when product was first used"
label variable country "Country where product was first used"
label variable mobile_source "Category describing user source"
label variable mobile_source_detail "More detailed descriptor of user source"
label variable device "Android device used to play the game"
label variable device_model "Model of Android device used to play game"
label variable latest_language "Latest language"
label variable initial_language "Language on device when product was first used"
label variable revenue_lvl1 "Cumulative revenue during level one of the game (before qualifying for promotional treatment, the game has 90 levels)"
label variable purchases_lvl1 "Cumulative purchases during level one of the game"
label variable sessions_lvl1 "Cumulative sessions played during level one of the game"
label variable seconds_lvl1 "Cumulative seconds spent in the game during level one of the game (not fully reliable, use with care)"
label variable rounds_lvl1 "Cumulative rounds played during level one of the game"
label variable active_hours_lvl1 "Cumulative hours that registered a login during level one of the game"
label variable active_days_lvl1 "Cumulative calendar days with a login during level one of the game"
label variable ads_lvl1 "Video ads watched during level one - measure generated from ads tracking table"
label variable conversion_lvl1 "Indicator if user made a purchase during level one of the game"
label variable coins_bought_lvl1 "Coins (in-game currency) bought at normal price during level one of the game"
label variable cash_bought_lvl1 "Cash (in-game currency) bought at normal price during level one of the game"
label variable coins_spent_lvl1 "Coins spent in the game during level one of the game"
label variable cash_spent_lvl1 "Cash spent in the game during level one of the game"
label variable min_coins_lvl1 "Minimum stock of coins recorded during level one of the game"
label variable max_coins_lvl1 "Maximum stock of coins recorded during level one of the game"
label variable avg_coins_lvl1 "Average stock of coins recorded during level one of the game"
label variable median_coins_lvl1 "Median stock of coins recorded during level one of the game"
label variable min_cash_lvl1 "Minimum stock of cash recorded during level one of the game"
label variable max_cash_lvl1 "Maximum stock of cash recorded during level one of the game"
label variable avg_cash_lvl1 "Average stock of cash recorded during level one of the game"
label variable median_cash_lvl1 "Median stock of cash recorded during level one of the game"
label variable revenue "Cumulative revenue until day 180 of product use"
label variable promo_revenue "Cumulative revenue until day 180 of product use from promotional purchases"
label variable shop_revenue "Cumulative revenue until day 180 of product use from non-promotional purchases"
label variable purchases "Cumulative purchases until day 180 of product use"
label variable promo_purchases "Cumulative promotional purchases until day 180 of product use"
label variable shop_purchases "Cumulative shop/non-promotional purchases until day 180 of product use"
label variable sessions "Cumulative sessions played until day 180 of product use"
label variable seconds_in_game "Cumulative seconds spent in the game until day 180 of product use (not fully reliable, use with care)"
label variable rounds_played "Cumulative rounds played until day 180 of product use"
label variable active_hours "Cumulative hours that registered a login until day 180 of product use"
label variable active_days "Cumulative calendar days with a login until day 180 of product use"
label variable conversion "Indicator if user made a purchase until day 180 of product use"
label variable coins_bought_normal "Coins (in-game currency) bought at normal price until day 180 of product use"
label variable cash_bought_normal "Cash (in-game currency) bought at normal price until day 180 of product use"
label variable coins_bought_low "Coins bought at promotional price until day 180 of product use"
label variable cash_bought_low "Cash bought at promotional price until day 180 of product use"
label variable coins_spent "Coins spent in the game until day 180 of product use"
label variable cash_spent "Cash spent in the game until day 180 of product use"
label variable min_coins "Minimum stock of coins recorded until day 180 of product use"
label variable max_coins "Maximum stock of coins recorded until day 180 of product use"
label variable avg_coins "Average stock of coins recorded until day 180 of product use"
label variable median_coins "Median stock of coins recorded until day 180 of product use"
label variable min_cash "Minimum stock of cash recorded until day 180 of product use"
label variable max_cash "Maximum stock of cash recorded until day 180 of product use"
label variable avg_cash "Average stock of cash recorded until day 180 of product use"
label variable median_cash "Median stock of cash recorded until day 180 of product use"
label variable days_first_promo_offer "Calendar days until first promo offer is shown to user"
label variable days_first_purchase "Calendar days until first purchase"
label variable days_first_promo_purchase "Calendar days until first promo purchase"
label variable days_first_shop_purchase "Calendar days until first purchase in the regular game shop"
label variable device_ram "Memory of device used to play game (higher RAM = higher device price on average)"
label variable x_resolution "Device resolution in x dimension"
label variable y_resolution "Device resolution in y dimension"
label variable dpi "DPI (dots per inch) of device, indicator of display quality"
label variable time_finish_scene "Time used to finish scenes (not fully reliable, use with care)"
label variable score_earned "Total score earned (not fully reliable, use with care)"
label variable badges_earned "Total badges earned on day t"
label variable scene_mastery "Average mastery displayed in playing scenes on day t"
label variable scenes_completed "Total number of scenes that were successfully completed (not fully reliable, use with care)"
label variable portfolio_d30rounds "Cumulative rounds played until day 30 in portfolio games all time (also outside observation/treatment period)"
label variable portfolio_d30rev "Cumulative revenue until day 30 in portfolio games all time (also outside observation/treatment period)"
label variable portfolio_d30devrev "Cumulative revenue until day 30 in portfolio games by users using the same device (all time, also outside observation/treatment period)"
label variable t0 "Indicator for immediate promotions treatment (after 0 days)"
label variable t25 "Indicator for intermediate promotions treatment (after 25 days)"
label variable t50 "Indicator for late promotions treatment (after 50 days)"
label variable c "Indicator for no promotions treatment (not until day 180 of lifetime)"
label variable log_revenue "Logarithm of revenue until day 180 + 1"
label variable log_rounds "Logarithm of rounds played until day 180 + 1"
label variable log_shop_revenue "Logarithm of non-promotional revenue until day 180 + 1"
label variable log_promo_revenue "Logarithm of promotional revenue until day 180 + 1"
label variable log_sessions "Logarithm of sessions until day 180 + 1"
label variable log_hours_in_game "Logarithm of hours spent in game until day 180 + 1 (not fully reliable, use with care)"
label variable log_seconds_in_game "Logarithm of seconds spent in game until day 180 + 1 (not fully reliable, use with care)"
label variable log_active_hours "Logarithm of hours with activity until day 180 + 1"
label variable us "Indicator if game downloaded in the US"
label variable uk "Indicator if game downloaded in the UK"
label variable de "Indicator if game downloaded in Germany"
label variable fr "Indicator if game downloaded in France"
label variable portfolio_revenue "Revenue generated by user in other large portfolio games during observation period"
label variable portfolio_purchases "Purchases made by user in other large portfolio games during observation period"
label variable portfolio_active_hours "Hours with activity by user in other large portfolio games during observation period"
label variable portfolio_sessions "Sessions by user in other large portfolio games during observation period"
label variable conversion_ts "Timestamp of first purchase"
label variable conversion_lvl1 "Indicator if user made purchase before completion of level one"
label variable treatment2 "Treatment user was assigned to"
label variable lvl1_complete_ts "Timestamp when user completed level one"
label variable lvl1plus_complete_ts "Timestamp when user complete level one or higher"
label variable lvl1_complete "Indicator if user chompleted level one (completion of level on is qualifying condition for exposure to treatment)"
label variable days_complete_lvl1 "Number of calendar days user spent before completing level one"
label variable coins_bought "Quantity of coins user bought for real money"
label variable cash_bought "Quantity of cash user bought for real money"
label variable days_in_game "Time spent in game in days (not fully reliable, use with care)"
label variable hours_in_game "Time spent in game in hours (not fully reliable, use with care)"
label variable hours_lvl1 "Time spent in game before completing level one in hours (not fully reliable, use with care)"
label variable ads "Video ads watched - measure 1 as generated from ads table"
label variable video_ads "Video ads watched - measure 2 as generated from user_base table"
label variable interstitial_ads "Interstitial ads exposed to - measure generated from user_base table"
label variable requests_sent "Requests sent on social network"
label variable ad_revenue "Revenue generated from ads watched by user"
label variable high_value "Exogenous indicator of high WTP users, derived from median split on device memory"
label variable high_engage "Pre-treatment indicator of high engagement users, derived from median split on calendar days needed to complete level one"
label variable overall_revenue "Total revenue, i.e. ad and IAP revenue"
label variable distinct_scenes_played "Distinct scenes played overall"
label variable total_badges "Total badges collected, up to 5 can be collected per scene (not fully reliable, use with care)"
label variable maximizer "Ratio of total badges and distinct scenes; indicator how much user is maximizer vs. satisficer (not fully reliable, use with care)"

// save dataset at dta file
save "/Data/x_xsection_cleaned.dta", replace

// create xsection only for treated users
drop if lvl1_complete==0
save "/Data/x_xsection_treated_cleaned.dta", replace
