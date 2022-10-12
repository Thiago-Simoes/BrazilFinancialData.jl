module FundsCVM

using ..DataFrames
using ..CSV
using ..HTTP
using ..ZipFile
using ..Dates
using ..Downloads
using ..XLSX
using ..MethodAnalysis

import JuliaInterpreter

# For fast debugging
push!(JuliaInterpreter.compiled_modules, Base)
visit(Base) do item
    isa(item, Module) && push!(JuliaInterpreter.compiled_modules, item)
    true
end

struct Fund
    fund_type::Union{String, Nothing}
    cnpj::String
    date::Date
    total_value::Float64
    quota_value::Float64
    net_worth::Float64
    deposits::Float64
    withdraws::Float64
    number_quota_holders::Int64
end

@enum HistoricalType HistoricalData RecentData


"""
    get_data_month(
        date::Date;
        generate_csv=false,
        csv_path="cvm_data.csv"    
    )::DataFrame

Get the data of all available funds in a specific month from CVM(Comissão de Valores Mobiliários)
database and returns formated in a DataFrame.

# Arguments
- `date`: the date of the desired month.
- `generate_csv`: if true will be generated a CSV file from the return DataFrame.
- `csv_path`: the path where the CSV file will be saved.

# Examples
julia> BrazilFinancialData.FundsCVM.get_data_month(Date(2020,6,10))  
julia> BrazilFinancialData.FundsCVM.get_data_month(Date(2020,6,10), generate_xlsx=true, xlsx_path="./FundsData.xlsx")  

"""
function get_data_month(
    date::Date;
    generate_csv=false,
    csv_path="cvm_data.csv"
)::DataFrame
    path = _get_data_path(date)
    ret = _get_data(path)
    
    if generate_csv
        CSV.write(csv_path, ret)
    end

    return ret
end


"""
    get_data_period(
        initial_date::Date,
        final_date::Date;
        include_begin=true,
        include_end=true,
        generate_csv=false,
        csv_path="cvm_data.csv"    
    )::DataFrame

Get the data of all available funds in a specific period from CVM(Comissão de Valores Mobiliários)
database and returns formated in a DataFrame. The data from CVM is monthly, but the function supports
specific days.

# Arguments
- `initial_date`: the initial date of the period.
- `final_date`: the final date of the period.
- `include_begin`: if true the first day will be include. Otherwise only the days after will.
- `include_end`: if true the last day will be include. Otherwise only the days before will.
- `generate_csv`: if true will be generated a CSV file from the return DataFrame.
- `csv_path`: the path where the CSV file will be saved.

# Example
julia> BrazilFinancialData.FundsCVM.get_data_period(Date(2020,6,10), Date(2020,8,14), include_end=false, include_begin=false)
"""
function get_data_period(
    initial_date::Date,
    final_date::Date;
    include_begin=true,
    include_end=true,
    generate_csv=false,
    csv_path="cvm_data.csv"
)::DataFrame
    initial_year = year(initial_date)
    final_year = year(final_date)

    @assert initial_date<final_date "'final_date' must be smaller than 'initial_date'."
    @assert final_date<=Dates.Date(Dates.now())

    years::Vector{Date} = []
    for year in initial_year:final_year
        push!(years, Date(year))
    end

    function foo(date::Date)::DataFrame
        return _get_data(_get_data_path(date), true)
    end

    op1 = include_begin ? (<=) : (<)
    op2 = include_end ? (<=) : (<)

    years_data = length(years) > 1 ? vcat((foo.(years))...) : _get_data(_get_data_path(years[1]), true)

    ret = years_data[(op1).(initial_date, years_data.DT_COMPTC) .* (op2).(years_data.DT_COMPTC, final_date), :]

    if generate_csv
        CSV.write(csv_path, ret)
    end

    return ret
end


"""
    function get_fund_data(
        fund_cnpj::String,
        initial_date::Date,
        final_date::Date;
        include_begin = true,
        include_end = true,
        only_quotes::Bool = false,
        generate_xlsx::Bool = false,
        xlsx_path::String = "",
        generate_csv::Bool = false,
        csv_path::String = ""
    )::DataFrame

Get the data of selected fund in a specific period from CVM(Comissão de Valores Mobiliários)
database and returns formated in a DataFrame. The data from CVM is monthly, but the function supports
specific days.

# Arguments
- `fund_cnpj`: a String, the CNPJ of the desired fund.
- `initial_date`: the initial date of the period.
- `final_date`: the final date of the period.
- `include_begin`: if true the first day will be include. Otherwise only the days after will.
- `include_end`: if true the last day will be include. Otherwise only the days before will.
- `generate_xlsx`: if true will be generated a XLSX file from the return DataFrame.
- `xlxs_path`: the path where the XLSX file will be saved.
- `generate_csv`: if true will be generated a CSV file from the return DataFrame.
- `csv_path`: the path where the CSV file will be saved.

# Example
julia> BrazilFinancialData.FundsCVM.get_fund_data("97.929.213/0001-34", Date(2021,1), Date(2021,2), include_end=false, generate_xlsx=true, xlsx_path = "fund.xlsx"
"""
function get_fund_data(
    fund_cnpj::String,
    initial_date::Date,
    final_date::Date;
    include_begin = true,
    include_end = true,
    only_quotes::Bool = false,
    generate_xlsx::Bool = false,
    xlsx_path::String = "",
    generate_csv::Bool = false,
    csv_path::String = ""
)::DataFrame
    if (generate_csv && generate_xlsx)
        @warn "You are generating CSV and XLSX at same time. This can lead to long waits."
    end
    df_data = (FundsCVM.get_data_period(initial_date, final_date, include_begin = include_begin, include_end = include_end))
    if only_quotes
        df_ret = df_data[df_data.CNPJ_FUNDO .== fund_cnpj, [:DT_COMPTC, :VL_QUOTA]]
    else
        df_ret = df_data[df_data.CNPJ_FUNDO .== fund_cnpj, :]
    end

    if generate_xlsx
        @assert (xlsx_path != "") "A xlsx_path must be passed."
        XLSX.writetable(xlsx_path, df_ret)
    end
    if generate_csv
        @assert (csv_path != "") "A csv_path must be passed."
        CSV.write(csv_path, df_ret)
    end
    
    return df_ret
end


function _get_data_path(date::Date)::Tuple{HistoricalType, String, String, Date}
    str_year = string(year(date))
    str_month = month(date) >= 10 ? string(month(date)) : "0"*string(month(date))
    
    if year(date)<2021
        str_link_path = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/inf_diario_fi_$(str_year).zip"
        type = HistoricalData
    else
        str_date = str_year*str_month
        str_link_path = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_$(str_date).zip"
        type = RecentData
    end

    filename = "inf_diario_fi_$str_year$str_month.csv"

    return (type, str_link_path, filename, date)
end

function _get_data(data_path::Tuple{HistoricalType, String, String, Date}, all_year::Bool = false)::DataFrame
    if all_year && year(data_path[4])<2021
        tmp_file = "$(tempname()).zip"
        Downloads.download(data_path[2], tmp_file)
        @assert isfile(tmp_file)

        zip = ZipFile.Reader(tmp_file)

        ret = DataFrame()
        for f in zip.files
            tmp = CSV.File(f, decimal='.', delim=';') |> DataFrame
            if columnindex(tmp, :TP_FUNDO) == 0
                tmp.TP_FUNDO = Vector{Missing}(undef, nrow(tmp))
            end
            ret = vcat(ret, tmp)
        end
        close(zip)
        rm(tmp_file)
        
        return ret
    elseif all_year
        ret = DataFrame()
        for month in 1:12
            ret = vcat(ret, get_data_month(Date(year(data_path[4]), month)))
        end
        return ret
    end
        

    tmp_file = "$(tempname()).zip"
    Downloads.download(data_path[2], tmp_file)
    @assert isfile(tmp_file)

    zip = ZipFile.Reader(tmp_file)

    for f in zip.files
        if f.name == data_path[3]
            ret = CSV.File(f, decimal='.', delim=';') |> DataFrame
            if columnindex(ret, :TP_FUNDO) == 0
                ret.TP_FUNDO = Vector{Missing}(undef, nrow(ret))
            end
            close(zip)
            rm(tmp_file)
            return ret
        end
    end

    close(zip)
    rm(tmp_file)
    error("$(data_path[3]) not found in $url")
    
end

end # Module FundsCVM