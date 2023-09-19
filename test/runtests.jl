using BrazilFinancialData
using Test
using DataFrames
using HTTP
using Gumbo
using Dates

println("Starting tests...")

df_test::DataFrame = FundsCVM.get_daily_inf_month(Date(2020,6,10))
@test sum(in.((["DT_COMPTC", "VL_QUOTA"]), [names(df_test)])) == 2

df_test::DataFrame = FundsCVM.get_daily_inf_period(Date(2020,6,10), Date(2020,8,14), include_end=false, include_begin=false)
@test sum(in.((["DT_COMPTC", "VL_QUOTA"]), [names(df_test)])) == 2

df_test::DataFrame = FundsCVM.get_fund_daily_inf("97.929.213/0001-34", Date(2021,1), Date(2021,2))
@test sum(in.((["DT_COMPTC", "VL_QUOTA"]), [names(df_test)])) == 2

@testset "FinancialStatementsCVM" begin
    @testset "getCVMCodes" begin
        cvmCodes = FinancialStatementsCVM.getCVMCodes()
        @test isa(cvmCodes, Dict)
        @test cvmCodes["C_021610"] == "021610 - B3 S.A. - BRASIL, BOLSA, BALCÃO (REGISTRO ATIVO)"
    end

    @testset "getCVMDocumentsCategories" begin
        cvmCategories = FinancialStatementsCVM.getCVMDocumentsCategories()
        @test isa(FinancialStatementsCVM.getCVMDocumentsCategories(), Vector{String})
        @test "EST_3" in cvmCategories
    end

    @testset "getCVMCompanyCategories" begin
        cvmCompanyCategories = FinancialStatementsCVM.getCVMCompanyCategories()
        @test isa(cvmCompanyCategories, Dict{String, String})
        @test "TODOS" in keys(cvmCompanyCategories)
    end

    @testset "getEnetConsultaExterna" begin
        @test isa(FinancialStatementsCVM.getEnetConsultaExterna(), HTMLDocument)
    end

    @testset "getSearchResults" begin
        # If lastRefDate true, only returns last report 
        searchResults = FinancialStatementsCVM.getSearchResults(codCvm=["021610"], category=["EST_3"], lastRefDate=false, startDate = Date(2000), endDate=today())
        @test isa(searchResults, DataFrame)
        @test nrow(searchResults) > 0
    end

    @testset "getReport" begin
        cvmCompanyCategories = FinancialStatementsCVM.getCVMCompanyCategories()
        reportsAll = FinancialStatementsCVM.getReport("130055", cvmCompanyCategories["TODOS"])
        @test isa(reportsAll, Dict)
        @test length(reportsAll) > 5
        reportsJustOne = FinancialStatementsCVM.getReport("130055", cvmCompanyCategories["TODOS"], ["Demonstração do Fluxo de Caixa"])
        @test isa(reportsJustOne, Dict)
        @test length(reportsJustOne) == 1
    end

    @testset "getASPSession" begin
        cvmCompanyCategories = FinancialStatementsCVM.getCVMCompanyCategories()
        @test isa(FinancialStatementsCVM.getASPSession("130055", cvmCompanyCategories["TODOS"]), Dict)
    end
end