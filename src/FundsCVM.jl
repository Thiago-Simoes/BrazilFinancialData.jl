# Summary
# 
# &&&&& Functions &&&&&
# get_daily_inf_month
# get_daily_inf_period
# get_fund_daily_inf
# get_statement_year
# get_statement_period
# get_fund_statement_period


module FundsCVM

using ..DataFrames
using ..CSV
using ..HTTP
using ..ZipFile
using ..Dates
using ..Downloads
using ..XLSX
using ..MethodAnalysis
using ..StringEncodings

include(string(@__DIR__,"/Utils.jl"))


# TODO: Remove this, is only for debugging
# import JuliaInterpreter

# # For fast debugging
# push!(JuliaInterpreter.compiled_modules, Base)
# visit(Base) do item
#     isa(item, Module) && push!(JuliaInterpreter.compiled_modules, item)
#     true
# end

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


blocosCDA = Dict(
    "1" => "TÍTULOS PÚBLICOS DO SELIC",
    "2" => "COTAS DE FUNDOS DE INVESTIMENTO",
    "3" => "SWAP",
    "4" => "DEMAIS ATIVOS CODIFICADOS",
    "5" => "DEPÓSITOS A PRAZO E OUTROS TÍTULOS DE IF",
    "6" => "TÍTULOS DO AGRONEGÓCIO E DE CRÉDITO PRIVADO",
    "7" => "INVESTIMENTO NO EXTERIOR",
    "8" => "DEMAIS ATIVOS NÃO CODIFICADOS"
)

"""
    get_cda_data(years::Vector{Int}; include_confidential::Bool = false)::Dict{Int, Dict{String, DataFrame}}

Baixa e lê os dados CDA da CVM para os anos especificados.

# Parâmetros
- `years`: Lista dos anos desejados.
- `include_confidential`: Se `true`, inclui os dados confidenciais.

# Retorno
- Um dicionário onde cada ano mapeia para um dicionário de blocos, e cada bloco mapeia para um DataFrame com os dados.

# Exemplo
```julia
cda_data = get_cda_data([2022, 2023])
"""
function get_cda_data(
    years::Vector{Int};
    include_confidential::Bool = false
)::Dict{Int, Dict{String, DataFrame}} # Dicionário principal: ano => Dict{bloco => DataFrame} 

    # Dicionário principal: ano => Dict{bloco => DataFrame}
    data_by_year = Dict{Int, Dict{String, DataFrame}}()

    for year in years
        data_by_year[year] = Dict{String, DataFrame}()
        
        for month in 1:12
            # Se o ano é maior que o ano atual, não tenta baixar
            current_year = Dates.year(Dates.today())
            current_month = Dates.month(Dates.today())
            if year > current_year || (year == current_year && month > current_month - 1)
                continue
            end
            
            month_str = lpad(string(month), 2, '0')
            date_str = string(year, month_str)
            
            # Nome do arquivo
            filename = "cda_fi_$date_str.zip"
            
            # URL do arquivo
            base_url = "https://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/"
            file_url = base_url * filename
            
            tmp_zip = ""  # Define tmp_zip antes do bloco try
            try
                # Cria um arquivo temporário para o ZIP
                tmp_zip = tempname() * ".zip"
                Downloads.download(file_url, tmp_zip)
                
                # Lê o arquivo ZIP
                zip = ZipFile.Reader(tmp_zip)
                
                for f in zip.files
                    # Ignorar arquivos que não sejam .csv
                    if !endswith(f.name, ".csv")
                        continue
                    end
                    
                    # Determina o bloco com base no nome do arquivo
                    if occursin("PL", f.name)
                        block_name = "PL"
                    elseif occursin("BLC", f.name)
                        m = match(r"cda_fi_BLC_(\d)_\d{6}\.csv", f.name)
                        if m !== nothing
                            block_number = m.captures[1]
                            block_name = "$(blocosCDA[string(block_number)])"
                        else
                            @warn "Nome de arquivo inesperado: $(f.name)"
                            continue
                        end
                    else
                        continue
                    end
                    
                    # Cria um arquivo temporário para o CSV
                    temp_csv_file = tempname() * ".csv"
                    open(temp_csv_file, "w") do io
                        write(io, read(f))
                    end
                    
                    # Lê o arquivo CSV usando o encoding correto
                    csv_io = read(temp_csv_file, enc"cp1252")
                    csv_data = CSV.File(csv_io, delim=';', dateformat="yyyy-mm-dd", decimal='.') |> DataFrame
                    
                    # Remove o arquivo CSV temporário após a leitura
                    rm(temp_csv_file; force=true)
                    
                    # Adiciona ao DataFrame existente
                    if haskey(data_by_year[year], block_name)
                        data_by_year[year][block_name] = vcat(data_by_year[year][block_name], csv_data)
                    else
                        data_by_year[year][block_name] = csv_data
                    end
                end
                # Fecha o arquivo ZIP antes de removê-lo
                close(zip)
                rm(tmp_zip; force=true)
            catch err
                if isa(err, HTTP.ExceptionRequest.StatusError) && err.status == 404
                    @warn "Arquivo não encontrado: $file_url"
                else
                    @warn "Falha ao baixar ou processar $file_url: $(err)"
                end
                # Remove o arquivo temporário se existir
                if tmp_zip != "" && isfile(tmp_zip)
                    try
                        rm(tmp_zip; force=true)
                    catch e
                        @warn "Não foi possível remover o arquivo temporário $tmp_zip: $(e)"
                    end
                end
            end
        end
    end
    return data_by_year    
end




"""
    search_cda_by_cnpj(cda_data::Dict{Int, Dict{String, DataFrame}}, cnpj::String)::Vector{DataFrame}

Busca nos dados CDA por registros correspondentes a um CNPJ específico.

# Parâmetros
- cda_data: Dados retornados por get_cda_data.
- cnpj: O CNPJ a ser buscado.
# Retorno
- Vetor de DataFrames contendo os registros correspondentes.
# Exemplo
```julia
fund_data = search_cda_by_cnpj(cda_data, "12.345.678/0001-90")
"""
function search_cda_by_cnpj(
    cda_data::Dict{Int, Dict{String, DataFrame}},
    cnpj::String
)::Tuple{Dict{Int, Dict{String, DataFrame}}, DataFrame, DataFrame}
    # Dicionário para armazenar os DataFrames filtrados
    results = Dict{Int, Dict{String, DataFrame}}()
    
    # DataFrame para acumular todas as posições
    all_positions = DataFrame()
    
    # Dicionário para armazenar o valor total por bloco
    block_totals = Dict{String, Float64}()
    
    total_value = 0.0  # Valor total acumulado das posições
    
    for (year, blocks) in cda_data
        filtered_blocks = Dict{String, DataFrame}()
        for (block_name, df) in blocks
            if "CNPJ_FUNDO" in names(df)
                # Filtra o DataFrame pelo CNPJ
                filtered_df = df[df.CNPJ_FUNDO .== cnpj, :]
                if nrow(filtered_df) > 0
                    # Adiciona ao dicionário de resultados
                    filtered_blocks[block_name] = filtered_df
                    
                    # Verifica se a coluna de valor está presente
                    if "VL_MERC_POS_FINAL" in names(filtered_df)
                        # Atualiza o valor total
                        total_value += sum(skipmissing(filtered_df.VL_MERC_POS_FINAL))
                        
                        # Acumula as posições
                        all_positions = vcat(all_positions, filtered_df, cols=:union)
                        
                        # Atualiza o total por bloco
                        block_value = sum(skipmissing(filtered_df.VL_MERC_POS_FINAL))
                        if haskey(block_totals, block_name)
                            block_totals[block_name] += block_value
                        else
                            block_totals[block_name] = block_value
                        end
                    end
                end
            end
        end
        if !isempty(filtered_blocks)
            results[year] = filtered_blocks
        end
    end
    
    # Calcula o percentual de cada posição
    if nrow(all_positions) > 0 && total_value > 0
        all_positions.Percentual = (all_positions.VL_MERC_POS_FINAL ./ total_value) .* 100
        # Ordena em ordem decrescente de percentual
        all_positions = sort(all_positions, :Percentual, rev=true)
    else
        @warn "Nenhuma posição encontrada para o CNPJ especificado."
    end
    
    # Cria o DataFrame com o percentual por bloco
    percentage_per_block = DataFrame(Block=String[], TotalValue=Float64[], Percentual=Float64[])
    for (block_name, block_value) in block_totals
        percent = (block_value / total_value) * 100
        push!(percentage_per_block, (Block=block_name, TotalValue=block_value, Percentual=percent))
    end
    # Ordena o DataFrame de percentuais por bloco
    percentage_per_block = sort(percentage_per_block, :Percentual, rev=true)
    
    return (results, all_positions, percentage_per_block)
end



""" 
    get_global_allocation(cda_data::Dict{Int, Dict{String, DataFrame}})::DataFrame

Calcula a alocação global dos ativos em todos os fundos e anos.

# Parâmetros
- cda_data: Dados retornados por get_cda_data.
# Retorno
DataFrame com os tipos de ativos, valor total de mercado e percentual.
# Exemplo
```julia
global_allocation = get_global_allocation(cda_data)
"""
function get_global_allocation(
    cda_data::Dict{Int, Dict{String, DataFrame}}
)::DataFrame
    allocation = DataFrame(TP_ATIVO=String[], VL_MERC_POS_FINAL=Float64[])
    for (year, blocks) in cda_data
        for (block_name, df) in blocks
            if "TP_ATIVO" in names(df) && "VL_MERC_POS_FINAL" in names(df)
                grouped_df = combine(groupby(df, :TP_ATIVO), :VL_MERC_POS_FINAL => sum => :Total)
                append!(allocation, grouped_df)
            end
        end
    end
    
    # Agrupa novamente para somar valores de anos e blocos diferentes
    final_allocation = combine(groupby(allocation, :TP_ATIVO), :Total => sum => :Total)
    
    # Calcula o percentual
    total_value = sum(final_allocation.Total)
    final_allocation.Percentual = (final_allocation.Total ./ total_value) .* 100
    
    return final_allocation
end    






"""
    get_daily_inf_month(
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
julia> BrazilFinancialData.FundsCVM.get_daily_inf_month(Date(2020,6,10))  
julia> BrazilFinancialData.FundsCVM.get_daily_inf_month(Date(2020,6,10), generate_xlsx=true, xlsx_path="./FundsData.xlsx")  

"""
function get_daily_inf_month(
    date::Date;
    generate_csv=false,
    csv_path="cvm_data.csv"
)::DataFrame
    path = _get_daily_inf_path(date)
    ret = _get_fund_quotes(path)
    
    if generate_csv
        CSV.write(csv_path, ret)
    end

    return ret
end


"""
    get_daily_inf_period(
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
julia> BrazilFinancialData.FundsCVM.get_daily_inf_period(Date(2020,6,10), Date(2020,8,14), include_end=false, include_begin=false)
"""
function get_daily_inf_period(
    initial_date::Date,
    final_date::Date;
    include_begin=true,
    include_end=true,
    generate_csv=false,
    csv_path="cvm_data.csv"
)::DataFrame
    initial_year = Dates.year(initial_date)
    final_year = Dates.year(final_date)

    @assert initial_date<final_date "'final_date' must be smaller than 'initial_date'."
    @assert final_date<=Dates.Date(Dates.now())

    years::Vector{Date} = []
    for year in initial_year:final_year
        push!(years, Date(year))
    end

    function foo(date::Date)::DataFrame
        return _get_fund_quotes(_get_daily_inf_path(date), true)
    end

    op1 = include_begin ? (<=) : (<)
    op2 = include_end ? (<=) : (<)

    years_data = length(years) > 1 ? vcat((foo.(years))...) : _get_fund_quotes(_get_daily_inf_path(years[1]), true)

    ret = years_data[(op1).(initial_date, years_data.DT_COMPTC) .* (op2).(years_data.DT_COMPTC, final_date), :]

    if generate_csv
        CSV.write(csv_path, ret)
    end

    return ret
end


"""
    get_fund_daily_inf(
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
julia> BrazilFinancialData.FundsCVM.get_fund_daily_inf("97.929.213/0001-34", Date(2021,1), Date(2021,2), include_end=false, generate_xlsx=true, xlsx_path = "fund.xlsx"
"""
function get_fund_daily_inf(
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
    df_data = (FundsCVM.get_daily_inf_period(initial_date, final_date, include_begin = include_begin, include_end = include_end))
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


"""
    get_statement_year(date::Date)::DataFrame

Get the statement of all funds in a specific year from CVM(Comissão de Valores Mobiliários)
database and returns formated in a DataFrame.

# Arguments
- `date`: a Date, the desired year.
# Example
julia> BrazilFinancialData.FundsCVM.get_statement_Dates.year(Date(2021))
"""
function get_statement_year(date::Date)::DataFrame
    path = _get_statement_path(date)
    df_ret = _get_fund_statement(path)
    return df_ret
end


"""
    get_statement_period(
        initial_date::Date,
        final_date::Date,
        include_begin::Bool=true,
        include_end::Bool=true
    )::DataFrame

Get the statement of all funds in a specific period of years from CVM(Comissão de Valores Mobiliários)
database and returns formated in a DataFrame.

# Arguments
- `initial_date`: a Date, the initial date.
- `final_date`: a Date, the final date.
- `final_date`: a Date, the final date.
- `include_begin`: if true the first day will be include. Otherwise only the days after will.
- `include_end`: if true the last day will be include. Otherwise only the days before will.

# Example
julia> BrazilFinancialData.FundsCVM.get_statement_period(Date(2020), Date(2021))
"""
function get_statement_period(
    initial_date::Date,
    final_date::Date,
    include_begin::Bool=true,
    include_end::Bool=true
)::DataFrame
    initial_year, final_year = Dates.year(initial_date), Dates.year(final_date)
    @assert initial_year<=final_year "`initial_date` must be smaller or equal `final_date`."
    years = collect(initial_year:final_year)

    function foo(x::Date)
        return _get_fund_statement(_get_statement_path(x))
    end

    df_ret = DataFrame()
    for year in years
        df_tmp = foo(Date(year))
        show(df_tmp)
        df_ret = vcat(df_ret, df_tmp)
    end

    op1 = include_begin ? (<=) : (<)
    op2 = include_end ? (<=) : (<)

    ret = df_ret[(op1).(initial_date, df_ret.DT_COMPTC) .* (op2).(df_ret.DT_COMPTC, final_date), :]

    return ret
end


"""
    get_fund_statement_period(
        fund_cnpj::String,
        initial_date::Date,
        final_date::Date,
        include_begin::Bool=true,
        include_end::Bool=true,
        generate_xlsx::Bool = false,
        xlsx_path::String = "",
        generate_csv::Bool = false,
        csv_path::String = ""
    )::DataFrame

Get the statement of a specific fund in a specific period of years from CVM(Comissão de Valores Mobiliários)
database and returns formated in a DataFrame. Can export CSV or/and XLSX files.

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
julia> BrazilFinancialData.FundsCVM.get_fund_statement_period(Date(2020), Date(2021))
"""
function get_fund_statement_period(
    fund_cnpj::String,
    initial_date::Date,
    final_date::Date,
    include_begin::Bool=true,
    include_end::Bool=true,
    generate_xlsx::Bool = false,
    xlsx_path::String = "",
    generate_csv::Bool = false,
    csv_path::String = ""
)::DataFrame
    initial_year, final_year = Dates.year(initial_date), Dates.year(final_date)

    @assert initial_year<=final_year "`initial_date` must be smaller or equal `final_date`."
    years = collect(initial_year:final_year)

    function foo(x::Date)
        return _get_fund_statement(_get_statement_path(x))
    end

    df_ret = DataFrame()
    for year in years
        df_tmp = foo(Date(year))
        df_ret = vcat(df_ret, df_tmp)
    end

    op1 = include_begin ? (<=) : (<)
    op2 = include_end ? (<=) : (<)

    df_ret = df_ret[(op1).(initial_date, df_ret.DT_COMPTC) .* (op2).(df_ret.DT_COMPTC, final_date), :]
    ret = df_ret[df_ret.CNPJ_FUNDO .== fund_cnpj, :]

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


function _get_statement_path(date::Date)::Tuple{String, Date}
    dt_year = Dates.year(date)
    @assert dt_year > 2014 "There is no data prior to 2015"
    str_year = string(dt_year)
    str_link = "https://dados.cvm.gov.br/dados/FI/DOC/EXTRATO/DADOS/extrato_fi_$(str_year).csv"
    return (str_link, date)
end


function _get_fund_statement(data_path::Tuple{String, Date})::DataFrame
    @assert Dates.year(data_path[2]) > 2014 "There is no data prior to 2014."

    tmp_file = "$(tempname()).csv"
    try
        Downloads.download(data_path[1], tmp_file)
        
        @assert isfile(tmp_file)
        
        ret = CSV.File(open(read, tmp_file, enc"CP1252"), decimal='.', delim=';') |> DataFrame

        DataFrames.sort!(ret, :DT_COMPTC)
        
        rm(tmp_file)
        return ret
    catch err
        error("No file for the year $(data_path[2]) found in $(data_path[1]).")
    end
end


function get_latest_fund_composition(fund_cnpj::String)::DataFrame
    @assert ndigits(clean_numbers(fund_cnpj, cnpj = true)) == 14 "The CNPJ must have 14 digits."

    ret = Dict{String, DataFrame}()
    date = Dates.today()

    while (ret == Dict{String, DataFrame}())
        tmp = _get_funds_sheet(date)
        if haskey(tmp, fund_cnpj)
            ret = tmp[fund_cnpj]
        end
        date = date - Dates.Month(1)
    end

    return ret    
end


function _get_funds_sheet(date::Dates.Date)::Dict{String, DataFrame}
    @assert Dates.year(date) > 2014 "There is no data prior to 2014."

    str_date = date_to_string(date)
    
    if Dates.year(date) < 2019
        endpoint = "https://dados.cvm.gov.br/dados/FI/DOC/LAMINA/DADOS/HIST/lamina_fi_"
    else
        endpoint = "https://dados.cvm.gov.br/dados/FI/DOC/LAMINA/DADOS/lamina_fi_"
    end

    url = endpoint * str_date * ".zip"
    tmp_file = "$(tempname()).zip"
    try
        Downloads.download(url, tmp_file)
        
        @assert isfile(tmp_file)

        for f in (ZipFile.Reader(tmp_file)).files
            if f.name == "lamina_fi_carteira_$str_date.csv"
                temp_str = "$(tempname()).csv"
                io = open(temp_str, "w")
                write(io, f)
                tmp = CSV.File(open(read, temp_str, enc"CP1252"), decimal='.', delim=';') |> DataFrame
                # tmp = CSV.File(f, decimal='.', delim=';') |> DataFrame
                DataFrames.sort!(tmp, :DT_COMPTC)
                transform!(tmp, 
                    :CNPJ_FUNDO => (x -> string.(x)) => :CNPJ_FUNDO
                )
                # Normalize data to make the sum equal 1(100%)
                # Some funds have a sum different than 1(100%)
                [tmp[tmp.CNPJ_FUNDO .== cnpj, "PR_PL_ATIVO"] ./= sum(tmp[tmp.CNPJ_FUNDO .== cnpj, "PR_PL_ATIVO"]) for cnpj in unique(tmp.CNPJ_FUNDO)];
                rm(tmp_file)
                rm(temp_str)
                ret = Dict([cnpj => tmp[tmp.CNPJ_FUNDO .== cnpj, [:DENOM_SOCIAL, :DT_COMPTC, :TP_ATIVO, :PR_PL_ATIVO]] for cnpj in unique(tmp.CNPJ_FUNDO)])
                return ret
            end
        end
    catch err
        println("No file for the date $(str_date) found in $(url).")
        Dict{String, DataFrame}()
    end
end


function _get_daily_inf_path(date::Date)::Tuple{HistoricalType, String, String, Date}
    str_year = string(Dates.year(date))
    str_month = month(date) >= 10 ? string(month(date)) : "0"*string(month(date))
    
    if Dates.year(date)<2021
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

function _get_fund_quotes(data_path::Tuple{HistoricalType, String, String, Date}, all_year::Bool = false)::DataFrame
    @assert Dates.year(data_path[4]) > 2004 "There is no data prior to 2005."

    if all_year && Dates.year(data_path[4])<2021
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
        year = Dates.year(data_path[4])
        max = year == Dates.year(Dates.today()) ? min(12, month(today()-Day(1))) : 12
        for month in 1:max
            ret = vcat(ret, get_daily_inf_month(Date(year, month)))
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