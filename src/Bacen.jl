# Summary
# 
# &&&&& Functions &&&&&
# get_daily_inf_month
# get_daily_inf_period
# get_fund_daily_inf
# get_statement_year
# get_statement_period
# get_fund_statement_period

# export get_daily_inf_month, get_daily_inf_period, get_fund_daily_inf, get_statement_year, get_statement_period, get_fund_statement_period

module Bacen

using DataFrames
using CSV
using HTTP
using ZipFile
using Dates
using Downloads
using XLSX
using MethodAnalysis
using StringEncodings

@enum BacenDataTypes money_BRL


const bacen_data_indx = Dict(
    :IPCA => (433, money_BRL)
)


function ipca(
    initial_date::Union{Nothing, Date} = nothing,
    final_date::Union{Nothing, Date} = nothing,
    generate_xlsx::Bool = false,
    xlsx_path::String = "",
    generate_csv::Bool = false,
    csv_path::String = ""
)::DataFrame
    ret = get_bacen_data(:IPCA, initial_date, final_date)
    if generate_xlsx
        @assert (xlsx_path != "") "A xlsx_path must be passed."
        XLSX.writetable(xlsx_path, ret)
    end
    if generate_csv
        @assert (csv_path != "") "A csv_path must be passed."
        CSV.write(csv_path, ret)
    end
    return ret
end


function get_bacen_data(
    indicator::Symbol,
    initial_date::Union{Date, Nothing} = nothing,
    final_date::Union{Date, Nothing} = nothing
)
    bacen_indx = bacen_data_indx[indicator]
    str_cod_bacen = string(bacen_indx[1])
    if typeof(initial_date) <: Nothing || typeof(final_date) <: Nothing
        str_path = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.$(str_cod_bacen)/dados?formato=csv"
    else
        str_path = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.$(str_cod_bacen)/dados?formato=csv&dataInicial=$(_convert_date_to_BR(initial_date))&dataFinal=$(_convert_date_to_BR(final_date))"
    end

    tmp_file = "$(tempname()).csv"

    Downloads.download(str_path, tmp_file)

    @assert isfile(tmp_file)

    df_ret = CSV.File(tmp_file, decimal='.', delim=';', stringtype=String) |> DataFrame

    if "data" in names(df_ret)
        rename!(df_ret, "data" => "Date")
        df_ret.Date = _convert_BR_to_date.(df_ret.Date)
    end
    
    if "valor" in names(df_ret)
        foo(x) = _data_converter(x, bacen_indx[2])
        df_ret.valor = foo.(df_ret.valor)
        rename!(df_ret, "valor" => string(indicator))
    end

    rm(tmp_file)

    return df_ret
end

function _data_converter(input::String, dataType::BacenDataTypes)::Float64
    if dataType == money_BRL
       return parse(Float64, replace(replace(replace(input, ',' => '*'), '.' => ','), '*' => '.'))
    end
end

function _convert_date_to_BR(date::Date)::String
    dia = day(date)>9 ? string(day(date)) : string(0, day(date))
    mes = month(date)>9 ? string(month(date)) : string(0, month(date))
    return string(dia, "/", mes, "/", year(date))
end

function _convert_BR_to_date(date::String)::Date
    data = split(date, "/")
    dia = parse(Int64, data[1])
    mes = parse(Int64, data[2])
    ano = parse(Int64, data[3])

    return Date(ano, mes, dia)
end

end # Module Bacen