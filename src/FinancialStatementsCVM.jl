module FinancialStatementsCVM

using ..Cascadia
using ..CSV
using ..DataFrames
using ..Dates
using ..Gumbo
using ..HTTP
using ..JSON
using ..PyCall
using ..StringEncodings
using ..ZipFile


export getCVMCodes, getCVMDocumentsCategories, getCVMCompanyCategories, getSearchResults, getReport

const REPORT_TYPE_MAPPER = Dict(
    "ITR" => "EST_3",
    "DFP" => "EST_4"
)

const BOOL_STRING_MAPPER = Dict(
    true => "true",
    false => "false"
)

const ENET_URL = "https://www.rad.cvm.gov.br/ENET/"
const ENETCONSULTA_URL = "https://www.rad.cvm.gov.br/ENETCONSULTA/"
const LISTAR_DOCUMENTOS_URL = "$(ENET_URL)frmConsultaExternaCVM.aspx/ListarDocumentos"
const ENET_CONSULTA_EXTERNA = "$(ENET_URL)frmConsultaExternaCVM.aspx"


function getCVMCodes()::Dict
    body = getEnetConsultaExterna()
    hdnEmpresasElement = eachmatch(sel"#hdnEmpresas", body.root)[1]
    hdnEmpresas = hdnEmpresasElement.attributes["value"]
    parsedData = hdnEmpresas |>
        x -> replace(x, "'" => "\"") |>
        x -> replace(x, "key" => "\"key\"") |>
        x -> replace(x, "value" => "\"value\"") |>
        x -> JSON.parse(x)
    
    parsedDict = Dict([company["key"] => company["value"] for company in parsedData])
    return parsedDict
end


function getCVMDocumentsCategories()::Vector{String}
    body = getEnetConsultaExterna()
    hdnCategorias = eachmatch(sel"#hdnComboCategoriaTipoEspecie", body.root)[1]
    companyCategories = hdnCategorias.attributes["value"] |> 
        parsehtml |>
        x -> eachmatch(sel"option", x.root[2]) .|>
        x -> x.attributes["value"] 
    return companyCategories
end


function getCVMCompanyCategories()::Dict{String, String}
    body = getEnetConsultaExterna()
    hdnCategorias = eachmatch(sel"#cboTipoParticipante", body.root)[1]
    companyCategories = hdnCategorias |>  x -> eachmatch(sel"option", x) .|>
        x -> (x[1].text |> strip |> string) => x.attributes["value"]
    return Dict(companyCategories)
end


function getEnetConsultaExterna()::HTMLDocument
    return HTTP.get(
        ENET_CONSULTA_EXTERNA,
        require_ssl_verification=false
    ).body |> String |> parsehtml
end


function getSearchResults(;
    codCvm::Vector{String},
    startDate::Date = Date(1),
    endDate::Date = Date(1),
    category::Vector{String},
    lastRefDate::Bool
)
    cvmCodesList_py = PyObject(codCvm) # List
    category_py = PyObject(category) # List
    lastRefDate_py = PyObject(lastRefDate) # Bool

    if startDate != Date(1)
        startDate_py = convertJLDateToPyDate(startDate) # date
    else
        startDate_py = PyObject("") # date
    end
    
    if endDate != Date(1)
        endDate_py = convertJLDateToPyDate(endDate) # date
    else
        endDate_py = PyObject("") # date
    end

    py_brFinance = PyCall.pyimport("brfinance")
    py_CVMAsyncBackend = py_brFinance.CVMAsyncBackend
    brFinanceCVM_HTTPClient = py_CVMAsyncBackend()

    returnPyDF = brFinanceCVM_HTTPClient.get_consulta_externa_cvm_results(
        cod_cvm = cvmCodesList_py,
        start_date = startDate_py,
        end_date = endDate_py,
        last_ref_date = lastRefDate_py,
        category = category_py
    )

    returnDF = convertPyDFToJLDF(returnPyDF)
    return returnDF   
end


function getReport(codigoSequencial::String, codigoTipoInstituicao::String, reportsList = [])
    URL::String = ENET_URL * "frmGerenciaPaginaFRE.aspx?NumeroSequencialDocumento=$(codigoSequencial)&CodigoTipoInstituicao=$(codigoTipoInstituicao)"
    bodyText::String = HTTP.get(URL, require_ssl_verification=false) |> x -> decode(x.body, "UTF-8")
    body = bodyText |> parsehtml

    hdnNumeroSequencialDocumento = eachmatch(Selector("#hdnNumeroSequencialDocumento"), body.root)[1].attributes["value"]
    hdnCodigoTipoDocumento = eachmatch(Selector("#hdnCodigoTipoDocumento"), body.root)[1].attributes["value"]
    hdnCodigoInstituicao = eachmatch(Selector("#hdnCodigoInstituicao"), body.root)[1].attributes["value"]
    hdnHash = eachmatch(Selector("#hdnHash"), body.root)[1].attributes["value"]
    
    numeroSequencialRegistroCVM = match(r"NumeroSequencialRegistroCvm=(.*?)&", bodyText).captures[1]
    endOfReportUrl = "&CodTipoDocumento=$(hdnCodigoTipoDocumento)&NumeroSequencialDocumento=$(hdnNumeroSequencialDocumento)&NumeroSequencialRegistroCvm=$(numeroSequencialRegistroCVM)&CodigoTipoInstituicao=$(hdnCodigoInstituicao)&Hash=$(hdnHash)"
    reports = Dict{String, Any}()
    
    opt = eachmatch(Selector("#cmbQuadro"), body.root)
    reportsOptions = eachmatch(Selector("option"), opt[1])
    
    if reportsList == []
        reportsList = [node[1].text for node in reportsOptions]
    end

    # Temporary
    # TODO: Fix this problem
    filter!(x -> x != "Demonstração das Mutações do Patrimônio Líquido", reportsList)
    
    for item in reportsOptions
        itemText = item[1].text
        if itemText in reportsList
            reportUrl = ENET_URL * item.attributes["value"] * endOfReportUrl
            reportHtmlResponse = getWithASPSession(
                codigoSequencial,
                codigoTipoInstituicao,
                reportUrl
            ).body |> String |> parsehtml
            tableElement::HTMLElement{:table} = reportHtmlResponse.root |> x -> eachmatch(sel"table", x) |> first
            reportDF = convertHTMLTableToDataFrame(tableElement)
            reports[itemText] = reportDF
        end
    end
    return reports
end


function getASPSession(codigoSequencial::String, codigoTipoInstituicao::String)::Dict
    url::String = "https://www.rad.cvm.gov.br/ENET/frmGerenciaPaginaFRE.aspx?NumeroSequencialDocumento=$(codigoSequencial)&CodigoTipoInstituicao=$(codigoTipoInstituicao)"
    req::HTTP.Messages.Response = HTTP.get(url, require_ssl_verification = false)
    cookies::Pair{SubString{String}, SubString{String}}  = req.request.headers[req.request.headers .|> (x -> x[1] == "Cookie")][1]
    pairKeyValueCookies = split(cookies[2], "; ") .|> x -> split(x, "=")
    dtCookies::Dict{Any, Any} = Dict()
    for (k,v) in pairKeyValueCookies
        if k == "ASP.NET_SessionId"
            dtCookies[k] = v
        end
    end
    return dtCookies
end


function getWithASPSession(codigoSequencial::String, codigoTipoInstituicao::String, url::String)::HTTP.Messages.Response
    headers = Dict("User-Agent" => "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    session::Dict = getASPSession(codigoSequencial, codigoTipoInstituicao)
    req::HTTP.Messages.Response = HTTP.get(replace(url, " " => ""), require_ssl_verification=false, cookies = session, headers = headers)
    return req
end

# Converters
function convertPyDFToJLDF(pythonDataFrame::PyObject)::DataFrame
    return DataFrames.DataFrame([col => collect(pythonDataFrame[col]) for col in pythonDataFrame.columns])
end


function convertJLDateToPyDate(date::Date)::PyObject
    py_datetime = PyCall.pyimport("datetime")
    return py_datetime.date(Dates.year(date), Dates.month(date), Dates.day(date))
end


function convertHTMLTableToDataFrame(htmlTable::HTMLElement{:table}, containHeader::Bool = true)
    header = String[]
    rows = Vector{String}[]
    for (i, row) in enumerate(eachmatch(sel"tr", htmlTable))
        cells = eachmatch(sel"td", row)
        if i == 1 && containHeader
            header = String[cleanSpacesOnString(strip(text(cell))) for cell in cells]
        else
            row_data = String[strip(text(cell)) for cell in cells]
            push!(rows, row_data)
        end
    end
    df = DataFrame(permutedims(hcat(rows...)), header, makeunique=true)
    return df
end


# Cleaning
function cleanSpacesOnString(str::AbstractString)::AbstractString
   str = replace(str, r"\s+" => " ")
end

end # FinancialStatementsCVM