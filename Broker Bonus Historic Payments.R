con <- DBI::dbConnect(odbc::odbc(), 'CLPImpala')

setwd("~/Broker Bonus")

# Pull Greenbook performance metrics
nabca <- dbGetQuery(con,
                    "SELECT 
t_date as 'date',
fy,
fy_yr_mo,
state_code,
state_number,
state_name,
distribution_type,
brand_num,
brand_full,
vendor_num,
vendor_abrev,
vendor_code,
vendor_name,
beverage_type,
category_lvl1,
bf_category_lvl1,
category_lvl2,
bf_category_lvl2,
category_lvl3,
bf_category_lvl3,
category_lvl4,
bf_category_lvl4,
case_9l,
shelf_dollar_vol,
shelf_dollar_vol_norm
FROM dp_data_science.vwr_nabca_greenbook 
WHERE fy >=2021
AND beverage_type = 'S'")



nabca2 <- nabca %>% filter(vendor_abrev == 'BROWN FORMAN') %>%  #& fy_yr_mo == 202210) %>% 
  group_by(fy_yr_mo,state_name,brand_full) %>% 
  summarise(vol = sum(case_9l))

unique(nabca$fy_yr_mo)

sort(unique(nabca2$state_name))
# #01/15/2021
# [1] "Alabama"               "Idaho"                 "Iowa"                  "Maine"                 "Montana"              
# [6] "Montgomery County,??? MD" "New Hampshire"         "North Carolina"        "Pennsylvania"          "Vermont"              
# [11] "Virginia"              "West Virginia" 
# 
# #01/26/2021
# [1] "Alabama"               "Idaho"                 "Iowa"                  "Maine"                 "Michigan"             
# [6] "Montana"               "Montgomery County, MD" "New Hampshire"         "North Carolina"        "Ohio"                 
# [11] "Oregon"                "Pennsylvania"          "Utah"                  "Vermont"               "Virginia"             
# [16] "West Virginia"  # MISSING MS, MT, & WY
# 
# #01/27/2021
# [1] "Alabama"               "Idaho"                 "Iowa"                  "Maine"                 "Michigan"             
# [6] "Mississippi"           "Montana"               "Montgomery County, MD" "New Hampshire"         "North Carolina"       
# [11] "Ohio"                  "Oregon"                "Pennsylvania"          "Utah"                  "Vermont"              
# [16] "Virginia"              "West Virginia"  #MISSING WY


y <- dbGetQuery(con,"DESCRIBE dp_bf_fin.tercur_incstmt")

tc <- dbGetQuery(con,"SELECT *
FROM dp_bf_fin.tercur_incstmt
WHERE fy >= 2021
and t_date <= now()
AND brand_lvl1 = 'Active Brands'
AND brand NOT IN ('ANTIGUO','JD RTDs Other','JD & COLA CONCENTRAT', 'JD COUNTRY COCKTAILS',
'JD SP CKTLS US&Canad','JDTH RTD', 'KORBEL CHAMPAGNE', 'Korbel Prosecco','LBD VODKA','MAXIMUS','TEQUILA NON-ALCOHOL',
'SONOMA-CUTRER')
AND value_type = '010'
AND lineitem_desc IN ('STRIPPED NET SALES', 'Broker Commissions','NET SALES')
AND market IN ('Alabama','Idaho', 'Iowa', 'Maine', 'Michigan', 'Mississippi', 'Montana', 'New Hampshire', 'North Carolina', 
'Ohio', 'Oregon', 'Pennsylvania', 'Utah', 'Vermont', 'Virginia', 'West Virginia', 'Wyoming', 'Montgomery County');")

# Read im commission Rates
rates <- dbGetQuery(con,"SELECT * FROM aa_route_to_consumer.tbl_broker_commission_rates")

rates <-  rates %>% rename(market_code = territory,
                          brand_code = brand,
                          comm_rate = commission_rate,
                          basis = commission_basis) %>% 
  mutate(market_code = as.character(market_code),
         brand_code = as.character(brand_code)) %>% 
  select(-basis)


df <- tc %>% select(fy,fy_month,fiscal_period,t_date,comp_code,market_code,market,brand_code,brand,lineitem_desc,amt) %>%
  mutate(tertile = case_when(fy_month == 1|
                               fy_month == 2|
                               fy_month == 3|
                               fy_month == 4 ~ 'T1',
                             fy_month == 5|
                               fy_month == 6|
                               fy_month == 7|
                               fy_month == 8 ~ 'T2',
                             fy_month == 9|
                               fy_month == 10|
                               fy_month == 11|
                               fy_month == 12 ~ 'T3')) %>% 
  group_by(fy,fy_month,fiscal_period,tertile,t_date,comp_code,market_code,market,brand_code,brand,lineitem_desc) %>%
  summarise(amt = sum(amt)) %>% 
  pivot_wider(names_from = lineitem_desc,values_from = amt) %>% 
  rename()
names(df) <- make.names(names(df))

df <- df %>%
  group_by(fy,fy_month,fiscal_period,market_code,market,brand_code,brand,tertile) %>% 
  summarise(sns = sum(STRIPPED.NET.SALES),
            ns = sum(NET.SALES),
            comm = sum(Broker.Commissions))


df <- left_join(df,rates) %>% filter(!is.na(comm_rate)) %>%
  mutate_if(is.numeric, ~ replace(.,is.na(.),0)) %>% 
  mutate(sns = round(sns,0),
         comm_calc = sns*comm_rate)

write_csv(df,'historical_payments.csv')

#NOT IN ('ANTIGUO', 'DON EDUARDO', 'JD RTDs Other','JD & COLA CONCENTRAT', 'JD COUNTRY COCKTAILS',
#'JD SP CKTLS US&Canad','JDTH RTD', 'KORBEL CHAMPAGNE', 'Korbel Prosecco','LBD VODKA','MAXIMUS','TEQUILA NON-ALCOHOL',
#'SONOMA-CUTRER')