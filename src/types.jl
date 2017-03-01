
immutable TableField
    name::Symbol # column name
    fieldtype::DataType # Julia type for this column
end

type TableSchema
    fields::Vector{TableField}
end

TableSchema(n::Integer) = TableSchema(Vector{TableField}(n))

function TableSchema(header::Vector{Symbol}, types::Vector{DataType})
    const n = length(header)
    @assert n == length(types) "Sizes mismatch: header = $n, types = $(length(types))"
    @assert n == length(unique(header)) "Column names must be unique"
    
    ts = TableSchema(n)
    for i in 1:n
        ts.fields[i] = TableField(header[i], types[i])
    end

    return ts
end

type Table <: AbstractTables.AbstractTable
    schema::TableSchema
    data::Vector{Any}

    function Table(s::TableSchema, d::Vector{Any})
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
