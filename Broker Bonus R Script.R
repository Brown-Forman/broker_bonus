 #title: Broker Bonus 
# Brenden McGale

library(odbc)
library(dplyr)
library(tidyverse)
library(stringr)
library(tidyr)
library(ranger)
library(feather)
library(Hmisc)
library(purrr)
library(RODBC)
library(stringi)
library(DBI)
library(RcppRoll)
library(naniar)

con <- DBI::dbConnect(odbc::odbc(), 'CLPImpala')

setwd("~/Broker Bonus")


# Net Sales from Territory Currency
tc <- dbGetQuery(con,"SELECT t_date as 'date', fy, fy_month, fiscal_period, t_date, brand, value_type, version, market, lineitem_desc, amt
FROM dp_bf_fin.tercur_incstmt
WHERE fy >= 2021
and t_date <= now()
AND brand_lvl1 = 'Active Brands'
AND value_type = '010'
AND lineitem_desc = 'STRIPPED NET SALES'
AND brand NOT IN ('ANTIGUO','JD RTDs Other','JD & COLA CONCENTRAT', 'JD COUNTRY COCKTAILS',
'JD SP CKTLS US&Canad','JDTH RTD', 'KORBEL CHAMPAGNE', 'Korbel Prosecco','LBD VODKA','MAXIMUS','TEQUILA NON-ALCOHOL',
'SONOMA-CUTRER')
AND market IN ('Alabama','Idaho', 'Iowa', 'Maine', 'Michigan', 'Mississippi', 'Montana', 'New Hampshire', 'North Carolina', 
'Ohio', 'Oregon', 'Pennsylvania', 'Utah', 'Vermont', 'Virginia', 'West Virginia', 'Wyoming', 'Montgomery County');")



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
WHERE fy >= 2021
AND beverage_type = 'S'")


#nabca & tc clean

n_df <- nabca %>% mutate(market = case_when(state_number == '18' ~ 'Montgomery County', TRUE ~ state_name),
                         vendor_name = case_when(vendor_name != 'BROWN FORMAN' ~ 'TDS', TRUE ~ 'BROWN FORMAN')) %>% 
  group_by(date,fy,fy_yr_mo,state_number,market,distribution_type,vendor_name) %>% 
  summarise(shelf_dollar_vol_norm = sum(shelf_dollar_vol_norm)) %>% ungroup() %>% 
  mutate(fiscal_period = as.character(fy_yr_mo),
         fiscal_period = case_when(fiscal_period == '201901' ~'2019001',
                                   str_trim(fiscal_period) == '201902' ~'2019002',
                                   str_trim(fiscal_period) == '201903' ~'2019003',
                                   str_trim(fiscal_period) == '201904' ~'2019004',
                                   str_trim(fiscal_period) == '201905' ~'2019005',
                                   str_trim(fiscal_period) == '201906' ~'2019006',
                                   str_trim(fiscal_period) == '201907' ~'2019007',
                                   str_trim(fiscal_period) == '201908' ~'2019008',
                                   str_trim(fiscal_period) == '201909' ~'2019009',
                                   str_trim(fiscal_period) == '201910' ~'2019010',
                                   str_trim(fiscal_period) == '201911' ~'2019011',
                                   str_trim(fiscal_period) == '201912' ~'2019012',
                                   str_trim(fiscal_period) == '202001' ~'2020001',
                                   str_trim(fiscal_period) == '202002' ~'2020002',
                                   str_trim(fiscal_period) == '202003' ~'2020003',
                                   str_trim(fiscal_period) == '202004' ~'2020004',
                                   str_trim(fiscal_period) == '202005' ~'2020005',
                                   str_trim(fiscal_period) == '202006' ~'2020006',
                                   str_trim(fiscal_period) == '202007' ~'2020007',
                                   str_trim(fiscal_period) == '202008' ~'2020008',
                                   str_trim(fiscal_period) == '202009' ~'2020009',
                                   str_trim(fiscal_period) == '202010' ~'2020010',
                                   str_trim(fiscal_period) == '202011' ~'2020011',
                                   str_trim(fiscal_period) == '202012' ~'2020012',
                                   str_trim(fiscal_period) == '202101' ~'2021001',
                                   str_trim(fiscal_period) == '202102' ~'2021002',
                                   str_trim(fiscal_period) == '202103' ~'2021003',
                                   str_trim(fiscal_period) == '202104' ~'2021004',
                                   str_trim(fiscal_period) == '202105' ~'2021005',
                                   str_trim(fiscal_period) == '202106' ~'2021006',
                                   str_trim(fiscal_period) == '202107' ~'2021007',
                                   str_trim(fiscal_period) == '202108' ~'2021008',
                                   str_trim(fiscal_period) == '202109' ~'2021009',
                                   str_trim(fiscal_period) == '202110' ~'2021010',
                                   str_trim(fiscal_period) == '202111' ~'2021011',
                                   str_trim(fiscal_period) == '202112' ~'2021012',
                                   str_trim(fiscal_period) == '202201' ~'2022001',
                                   str_trim(fiscal_period) == '202202' ~'2022002',
                                   str_trim(fiscal_period) == '202203' ~'2022003',
                                   str_trim(fiscal_period) == '202204' ~'2022004',
                                   str_trim(fiscal_period) == '202205' ~'2022005',
                                   str_trim(fiscal_period) == '202206' ~'2022006',
                                   str_trim(fiscal_period) == '202207' ~'2022007',
                                   str_trim(fiscal_period) == '202208' ~'2022008',
                                   str_trim(fiscal_period) == '202209' ~'2022009',
                                   str_trim(fiscal_period) == '202210' ~'2022010',
                                   str_trim(fiscal_period) == '202211' ~'2022011',
                                   str_trim(fiscal_period) == '202212' ~'2022012',
                                   str_trim(fiscal_period) == '202301' ~'2023001',
                                   str_trim(fiscal_period) == '202302' ~'2023002',
                                   str_trim(fiscal_period) == '202303' ~'2023003',
                                   str_trim(fiscal_period) == '202304' ~'2023004',
                                   str_trim(fiscal_period) == '202305' ~'2023005',
                                   str_trim(fiscal_period) == '202306' ~'2023006',
                                   str_trim(fiscal_period) == '202307' ~'2023007',
                                   str_trim(fiscal_period) == '202308' ~'2023008',
                                   str_trim(fiscal_period) == '202309' ~'2023009',
                                   str_trim(fiscal_period) == '202310' ~'2023010',
                                   str_trim(fiscal_period) == '202311' ~'2023011',
                                   str_trim(fiscal_period) == '202312' ~'2023012',
                                   TRUE ~ str_trim(fiscal_period)))


tc_df <- tc %>% group_by(date,fy,fy_month,fiscal_period,market) %>%
  summarise('net_sales'= sum(amt)) %>% ungroup() %>% 
  mutate(vendor_name = 'BROWN FORMAN')

df <- left_join(n_df,tc_df, by = c('date','fy','market','vendor_name','fiscal_period')) %>% write_csv('Broker_Bonus.csv')



