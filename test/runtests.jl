using BrazilFinancialData
using Test
using DataFrames
using Dates

println("Starting tests...")

# Next addition will be better tests.

# TODO: Complete this tests with integrity verifications and testing for the new functions.

df_test::DataFrame = FundsCVM.get_daily_inf_month(Date(2020,6,10))
@test sum(in.((["DT_COMPTC", "VL_QUOTA"]), [names(df_test)])) == 2

df_test::DataFrame = FundsCVM.get_daily_inf_period(Date(2020,6,10), Date(2020,8,14), include_end=false, include_begin=false)
@test sum(in.((["DT_COMPTC", "VL_QUOTA"]), [names(df_test)])) == 2

df_test::DataFrame = FundsCVM.get_fund_daily_inf("97.929.213/0001-34", Date(2021,1), Date(2021,2))
@test sum(in.((["DT_COMPTC", "VL_QUOTA"]), [names(df_test)])) == 2