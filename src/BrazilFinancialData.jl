module BrazilFinancialData

using DataFrames
using Dates
using Cascadia
using CSV
using HTTP
using Gumbo
using ZipFile
using Dates
using Downloads
using XLSX
using MethodAnalysis
using StringEncodings
using JSON
using PyCall
using TimeSeries

include("FundsCVM.jl")
include("Bacen.jl")
include("FinancialStatementsCVM.jl")
include("MarketData.jl")

export FundsCVM
export Bacen
export FinancialStatementsCVM

end # module BrazilFinancialData
