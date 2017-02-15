
type Schema
    names::Vector{Symbol}       # column names
    types::Vector{DataType}      # Julia types of columns

    function Schema(header::Vector{Symbol}, types::Vector{DataType})
        @assert length(header) == length(types) "Sizes mismatch: header = $(length(header)), types = $(length(types))"
        @assert length(header) == length(unique(header)) "Column names must be unique"
        new(header, types)
    end
end

type Table #<: AbstractDataFrame
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

immutable TableRow
    table::Table
    row::Int
end

immutable TableRowIterator
    table::Table
end

type CSVFormat
    dlm::Char
    decimal_separator::Char
    thousands_separator::Nullable{Char}
    null_str::String
    date_format::Dates.DateFormat

    function CSVFormat(dlm::Char, decimal_separator::Char, thousands_separator::Nullable{Char}, null_str::String, date_format::Dates.DateFormat)
        # Checks if input is consistent
        if !isnull(thousands_separator)
            ts_char = get(thousands_separator)
            if ts_char == decimal_separator
                error("decimal_separator ($decimal_separator) conflicts with thousands_separator ($ts_char).")
            end
        end
        new(dlm,decimal_separator,thousands_separator,null_str,date_format)
    end
end

# Create with default values
CSVFormat(; dlm::Char=';', decimal_separator::Char=',', thousands_separator::Nullable{Char}=Nullable{Char}(), null_str::String="", date_format::Dates.DateFormat=Dates.ISODateFormat) = CSVFormat(dlm, decimal_separator, thousands_separator, null_str, date_format)
isnullstr(str, fm::CSVFormat) = str == fm.null_str
