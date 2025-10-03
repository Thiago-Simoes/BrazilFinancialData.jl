module Bacen

using ..DataFrames
using ..CSV
using ..HTTP
using ..ZipFile
using ..Dates
using ..Downloads
using ..XLSX
using ..MethodAnalysis
using ..StringEncodings
using ..JSON

@enum BacenDataTypes Money_BRL Percentage Values


const bacen_data_indx = Dict(
    :IPCA => (433, Money_BRL),
    :SELIC => (1178, Percentage),
    :USD_BRL => (10813, Money_BRL),
    :CAGED => (28763, Values),
    :SINAPI => (7495, Percentage),
    :PIB => (1207, Money_BRL),
    :IC_BR => (27574, Values),
    :UtilizacaoCapacidadeIndustrial => (1344, Percentage),
    :Desemprego => (24369, Percentage),
    :DividaLiq => (2053, Money_BRL),
    :DividaLiqPIB => (4503, Percentage),
    :CDI_Diario => (12, Percentage),
    :CDI_Diario_Anualizado => (4389, Percentage),
    # Cestas basicas
    :CestaBasicaAracaju => (7479, Money_BRL), 
    :CestaBasicaBelem => (7480, Money_BRL), 
    :CestaBasicaBeloHorizonte => (7481, Money_BRL), 
    :CestaBasicaBrasilia => (7482, Money_BRL), 
    :CestaBasicaCuritiba => (7483, Money_BRL), 
    :CestaBasicaFlorianopolis => (7484, Money_BRL), 
    :CestaBasicaFortaleza => (7485, Money_BRL), 
    :CestaBasicaGoiania => (7486, Money_BRL), 
    :CestaBasicaJoaoPessoa => (7487, Money_BRL), 
    :CestaBasicaNatal => (7488, Money_BRL), 
    :CestaBasicaPortoAlegre => (7489, Money_BRL), 
    :CestaBasicaRecife => (7490, Money_BRL), 
    :CestaBasicaRJ => (7491, Money_BRL), 
    :CestaBasicaSalvador => (7492, Money_BRL), 
    :CestaBasicaSaoPaulo => (7493, Money_BRL), 
    :CestaBasicaVitoria => (7494, Money_BRL), 
)


ipca(parms::Vector = [])::DataFrame = return get_indicator(:IPCA, parms...)
selic(parms::Vector = [])::DataFrame = return get_indicator(:SELIC, parms...)
usd_brl(parms::Vector = [])::DataFrame = return get_indicator(:USD_BRL, parms...)
caged(parms::Vector = [])::DataFrame = return get_indicator(:CAGED, parms...)
sinapi(parms::Vector = [])::DataFrame = return get_indicator(:SINAPI, parms...)
pib(parms::Vector = [])::DataFrame = return get_indicator(:PIB, parms...)
ic_br(parms::Vector = [])::DataFrame = return get_indicator(:IC_BR, parms...)
utilizacao_capacidade_industrial(parms::Vector = [])::DataFrame = return get_indicator(:UtilizacaoCapacidadeIndustrial, parms...)
desemprego(parms::Vector = [])::DataFrame = return get_indicator(:Desemprego, parms...)
divida_liq(parms::Vector = [])::DataFrame = return get_indicator(:DividaLiq, parms...)
divida_liq_pib(parms::Vector = [])::DataFrame = return get_indicator(:DividaLiqPIB, parms...)
cdi_diario(parms::Vector = [])::DataFrame = return get_indicator(:CDI_Diario, parms...)
cdi_diario_anualizado(parms::Vector = [])::DataFrame = return get_indicator(:CDI_Diario_Anualizado, parms...)





function get_indicator(
    indicator::Union{Symbol, Int64},
    initial_date::Union{Nothing, Date} = nothing,
    final_date::Union{Nothing, Date} = nothing,
    generate_xlsx::Bool = false,
    xlsx_path::String = "",
    generate_csv::Bool = false,
    csv_path::String = ""
)::DataFrame
    ret = _get_bacen_data(indicator, initial_date, final_date)
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

function _get_bacen_data(
    indicator::Union{Symbol, Int64}, 
    start_date::Union{Date, Nothing} = nothing, 
    end_date::Union{Date, Nothing} = nothing; 
    max_days::Int = 600
)::DataFrame
    # Se `start_date` ou `end_date` forem `nothing`, define os valores padrão
    start_date = start_date === nothing ? Date("2000-01-01") : start_date
    end_date = end_date === nothing ? today() : end_date
    
    # Verifica se as datas são válidas
    if start_date > end_date
        throw(ArgumentError("A data inicial deve ser anterior ou igual à data final."))
    end

    results = DataFrame[]  # Lista para armazenar resultados por bloco
    failed_blocks = []  # Para armazenar os blocos que falharem
    current_start = start_date  # Data inicial para o loop

    while current_start <= end_date
        # Calcula o final do bloco, respeitando `max_days`
        current_end = min(current_start + Day(max_days - 1), end_date)

        # Tenta buscar os dados, com retentativa
        success = false
        for attempt in 1:2
            try
                data = _get_bacen_data_raw(indicator, current_start, current_end)
                @assert ("Date" in names(data))
                push!(results, data)
                success = true
                break  # Sai do loop de retentativa
            catch e
                sleep(0.1)  # Aguarda 0,1 segundos antes da próxima tentativa
            end
        end

        # Se falhar após as tentativas, registra o bloco como falhado
        if !success
            push!(failed_blocks, (current_start, current_end))
        end

        # Avança para o próximo bloco
        current_start = current_end + Day(1)
    end

    # Concatena os resultados e ordena por data (se existir coluna "Date")
    final_result = vcat(results...)
    if "Date" in names(final_result)
        sort!(final_result, :Date)
    end

    return final_result
end


function _get_bacen_data_raw(
    indicator::Union{Symbol, Int64},
    initial_date::Union{Date, Nothing} = nothing,
    final_date::Union{Date, Nothing} = nothing
)::DataFrame
    type = ""
    if typeof(indicator) <: Symbol
        bacen_indx = bacen_data_indx[indicator]
        str_cod_bacen = string(bacen_indx[1])
        type = bacen_indx[2]
    else 
        str_cod_bacen = string(indicator)
        type = Values
    end
    str_path = ""
    if typeof(initial_date) <: Nothing || typeof(final_date) <: Nothing
        str_path = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.$(str_cod_bacen)/dados?formato=json"
    else
        str_path = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.$(str_cod_bacen)/dados?formato=json&dataInicial=$(_convert_date_to_BR(initial_date))&dataFinal=$(_convert_date_to_BR(final_date))"
    end

    tmp_file = "$(tempname()).json"

    Downloads.download(str_path, tmp_file)

    @assert isfile(tmp_file)

    df_ret = JSON.read(tmp_file) |> String |> JSON.parse |> DataFrames.DataFrame

    if "data" in names(df_ret)
        rename!(df_ret, "data" => "Date")
        df_ret.Date = _convert_BR_to_date.(df_ret.Date)
    end
    
    if "valor" in names(df_ret)
        foo(x) = parse(Float64, x)
        df_ret.valor = foo.(df_ret.valor)
        rename!(df_ret, "valor" => string(indicator))
    end

    rm(tmp_file)

    return df_ret
end

function _data_converter(input::AbstractString, dataType::BacenDataTypes)::Float64
    if dataType == Money_BRL
       return _parse_from_brazilian_to_float64(input)
    elseif dataType == Percentage
        return (_parse_from_brazilian_to_float64(input))/100
    elseif dataType == Values
        return (_parse_from_brazilian_to_float64(input))
    end
end

function _data_converter(input::Int, dataType::BacenDataTypes)::Float64
    return Float64(input)
end

_parse_from_brazilian_to_float64(x::AbstractString)::Float64 = parse(Float64, replace(replace(replace(x, ',' => '*'), '.' => ','), '*' => '.'))
_parse_from_brazilian_to_float64(x::Int)::Float64 = x * 1.0

function _convert_date_to_BR(date::Date)::String
    dia = day(date)>9 ? string(day(date)) : string(0, day(date))
    mes = month(date)>9 ? string(month(date)) : string(0, month(date))
    return string(dia, "/", mes, "/", year(date))
end

function _convert_BR_to_date(date::AbstractString)::Date
    data = split(date, "/")
    dia = parse(Int64, data[1])
    mes = parse(Int64, data[2])
    ano = parse(Int64, data[3])

    return Date(ano, mes, dia)
end

export get_indicator, ipca, selic, usd_brl, caged, sinapi, pib, ic_br, utilizacao_capacidade_industrial, desemprego, divida_liq, divida_liq_pib

end # Module Bacen
