using BrazilFinancialData
using Test
using DataFrames
using Dates

println("Starting tests...")

# Next addition will be better tests.

df_test::DataFrame = BrazilFinancialData.FundsCVM.get_data_month(Date(2020,6,10))
@test sum(in.((["DT_COMPTC", "VL_QUOTA"]), [names(df_test)])) == 2

df_test::DataFrame = BrazilFinancialData.FundsCVM.get_data_period(Date(2020,6,10), Date(2020,8,14), include_end=false, include_begin=false)
@test sum(in.((["DT_COMPTC", "VL_QUOTA"]), [names(df_test)])) == 2

df_test::DataFrame = BrazilFinancialData.FundsCVM.get_fund_data("97.929.213/0001-34", Date(2021,1), Date(2021,2))
@test sum(in.((["DT_COMPTC", "VL_QUOTA"]), [names(df_test)])) == 2