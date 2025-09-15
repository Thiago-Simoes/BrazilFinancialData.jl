module InternationalSources

using Dates
using DataFrames

root = @__DIR__
import Pkg
Pkg.activate(joinpath(root, "wrap/Yahoo"))
Pkg.instantiate()

include(joinpath(root, "wrap/Yahoo/src/MarketData.jl"))

import .MarketData

export YahooOpt, yahoo

function stocks(symbol::AbstractString; start_date::Date=Date(2000), end_date::Date=today(), ssl_verification::Bool = true)::TimeArray
  opt = MarketData.YahooOpt(period1=DateTime(start_date), period2=DateTime(end_date))
  return MarketData.yahoo(symbol, opt, ssl_verification) |> DataFrame
end

stocks(symbol::Symbol; start_date::Date=Date(2000), end_date::Date=today(), ssl_verification::Bool = true)::TimeArray =
  stocks(string(symbol); start_date=start_date, end_date=end_date, ssl_verification=ssl_verification)

function fred(series::AbstractString; start_date::Date=Date(2000), end_date::Date=today(), ssl_verification::Bool = true)::TimeArray
  data = MarketData.fred(series, opt, ssl_verification)
  filter!(x -> start_date <= Date(x.timestamp) <= end_date, data)
  return data |> DataFrame
end
fred(series::Symbol; start_date::Date=Date(2000), end_date::Date=today(), ssl_verification::Bool = true)::TimeArray =
  return fred(string(series); start_date=start_date, end_date=end_date, ssl_verification=ssl_verification) |> DataFrame

end