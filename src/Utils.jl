function date_to_string(date::Date)::String
    month = Dates.month(date) > 9 ? string(Dates.month(date)) : string(0, Dates.month(date))
    return string(Dates.year(date)) * month
end


function clean_numbers(input::String; brl::Bool = false, cnpj::Bool = false)::Union{Float64, Int64}
    if brl
        return parse(Float64, replace(replace(replace(replace(input, ',' => '*'), '.' => ','), '*' => '.'),  r"[^0-9.]" => ""))
    elseif cnpj
        return parse(Int64, replace(replace(input, r"[^0-9.]" => ""), "." => ""))
    else
        return parse(Float64, replace(input, r"[^0-9.]" => ""))
    end
end