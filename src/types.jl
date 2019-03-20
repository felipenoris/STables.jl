
mutable struct Schema
    names::Vector{Symbol}       # column names
    types::Vector{Type}      # Julia types of columns

    function Schema(header::Vector{Symbol}, types::Vector{Type})
        @assert length(header) == length(types) "Sizes mismatch: header = $(length(header)), types = $(length(types))"
        @assert length(header) == length(unique(header)) "Column names must be unique"
        new(header, types)
    end
end

mutable struct Table #<: AbstractDataFrame
    schema::Schema
    data::Vector{Any}

    function Table(s::Schema, d::Vector{Any})
        @assert length(s.names) == length(d)
        for i = 1:length(s.types)
            @assert eltype(d[i]) == s.types[i] "Vector types don't match table's schema."
        end
        new(s,d)
    end
end

struct TableRow
    table::Table
    row::Int
end

struct TableRowIterator
    table::Table
end

check_ds_ts_clash(decimal_separator::Char, thousands_separator::Nothing) = nothing
function check_ds_ts_clash(decimal_separator::Char, thousands_separator::Char)
    @assert thousands_separator != decimal_separator "decimal_separator ($decimal_separator) conflicts with thousands_separator ($thousands_separator)."
end

mutable struct CSVFormat
    dlm::Char
    decimal_separator::Char
    thousands_separator::Union{Nothing, Char}
    missing_str::String
    date_format::Dates.DateFormat

    function CSVFormat(dlm::Char, decimal_separator::Char, thousands_separator::Union{Nothing, Char}, missing_str::String, date_format::Dates.DateFormat)
        check_ds_ts_clash(decimal_separator, thousands_separator)
        new(dlm, decimal_separator, thousands_separator, missing_str, date_format)
    end
end

# Create with default values
CSVFormat(; dlm::Char=';', decimal_separator::Char=',', thousands_separator::Union{Nothing, Char}=nothing, missing_str::String="", date_format::Dates.DateFormat=Dates.ISODateFormat) = CSVFormat(dlm, decimal_separator, thousands_separator, missing_str, date_format)
ismissingstr(str, fm::CSVFormat) = str == fm.missing_str
