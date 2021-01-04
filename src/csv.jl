
"""Converts 100.000.000,00 to 100000000,00"""
@inline function remove_thousands_separator(value::T, ts::Nothing) :: T where {T<:AbstractString}
    return value
end

@inline function remove_thousands_separator(value::T, ts::Char) :: T where {T<:AbstractString}
    return replace(value, ts => "")
end

"""Converts 1000,00 to 1000.00"""
@inline function fix_decimal_separator(value::T, ds::Char) :: T where {T<:AbstractString}
    if ds != '.'
        return replace(value, ds => '.')
    else
        return value
    end
end

@inline function _parse_raw_value(raw_value::String, ::Type{T}, format::CSVFormat) where {T<:AbstractString}
    stripped = strip(raw_value)

    # strips '"' character if on both sides of string
    if startswith(stripped, '"') && endswith(stripped, '"')
        return strip(stripped, '"')
    else
        return stripped
    end
end

@inline function _parse_raw_value(raw_value::String, ::Type{T}, format::CSVFormat) where {T<:Number}
    value = remove_thousands_separator(raw_value, format.thousands_separator)
    value = fix_decimal_separator(value, format.decimal_separator)
    return parse(T, value)
end

@inline function _parse_raw_value(raw_value::String, ::Type{T}, format::CSVFormat) where {T<:Dates.TimeType}
    return Date(raw_value, format.date_format)
end

@inline function _parse_raw_value(raw_value::String, ::Type{Union{Missing, T}}, format::CSVFormat) where {T}
    if raw_value == format.missing_str
        return missing
    else
        return _parse_raw_value(raw_value, T, format)
    end
end

# doesn't know how to parse other types
function _parse_raw_value(raw_value::String, ::Type{T}, format::CSVFormat) where {T}
    error("Parsing $T not implemented.")
end

function _read_column(raw_column::Vector{String}, ::Type{T}, ROW_OFFSET::Int, FST_DATAROW_INDEX::Int,
    rows::Int, format::CSVFormat) where {T}

    col_data = _create_table_column(T, rows + ROW_OFFSET)
    @inbounds for r in FST_DATAROW_INDEX:rows
        r_ = r + ROW_OFFSET # r_ is the line index of the destination table. If raw contains a header, r_ = r - 1 . Otherwise, r_ = r
        col_data[r_] = _parse_raw_value(raw_column[r], T, format)
    end
    return col_data
end

const REGEX_WITH_QUOTES = r"^\s*\"\s*(?<str>.*)\s*\"\s*$"
const REGEX_WITHOUT_QUOTES = r"^\s*(?<str>.*)\s*$"

function _read_data!(table::Table, raw::Array{String,2}, format::CSVFormat; header::Bool=true)

    rows, cols = size(raw)

    # Check if header is consistent with schema
    @assert cols == length(table.schema.names) "CSV not consistent with given schema. ncols in file: $cols; ncols in schema: $(length(table.schema.names))."

    if header
        ROW_OFFSET = -1
    else
        ROW_OFFSET = 0
    end

    FST_DATAROW_INDEX = 1 - ROW_OFFSET # this is the index of the first data row in $raw

    col_array = Vector{Any}(undef, cols)
    @inbounds for col in 1:cols
        try
            col_type = table.schema.types[col]
            col_array[col] = _read_column(raw[:,col], col_type, ROW_OFFSET, FST_DATAROW_INDEX, rows, format)
        catch err
            @error("Error parsing column $col - $(table.schema.names[col]) <: $(table.schema.types[col])")
            rethrow()
        end
    end

    # Set table data without checks
    table.data = col_array
    return table
end

function _readraw(input, format::CSVFormat, use_mmap::Bool, skipstart, comments::Bool, comment_char::Char)
    return DelimitedFiles.readdlm(input, format.dlm, String; use_mmap=use_mmap, skipstart=skipstart, comments=comments, comment_char=comment_char)
end

"""
    readcsv(input, schema::Schema, format::CSVFormat=CSVFormat(); header::Bool=true, use_mmap::Bool=false, skipstart=0, comments::Bool=false, comment_char::Char='#')

`header` Tells if the input file has a header in the first line. Default is `true`.
"""
function readcsv(input, schema::Schema, format::CSVFormat=CSVFormat(); header::Bool=true, use_mmap::Bool=false, skipstart=0, comments::Bool=false, comment_char::Char='#')
    raw = _readraw(input, format, use_mmap, skipstart, comments, comment_char)
    tb = Table(schema)
    return _read_data!(tb, raw, format; header=header)
end

"""
    readcsv(input, format::CSVFormat=CSVFormat(); header::Bool=true, use_mmap::Bool=false, skipstart=0, comments::Bool=false, comment_char::Char='#')

Uses Schema inference.
"""
function readcsv(input, format::CSVFormat=CSVFormat(); header::Bool=true, use_mmap::Bool=false, skipstart=0, comments::Bool=false, comment_char::Char='#')
    raw = _readraw(input, format, use_mmap, skipstart, comments, comment_char)
    schema = infer_schema(raw, format, header)
    tb = Table(schema)
    return _read_data!(tb, raw, format; header=header)
end

function readcsv(input, types::Vector{T}, format::CSVFormat=CSVFormat(); use_mmap::Bool=false, skipstart=0, comments::Bool=false, comment_char::Char='#') where {T<:Type}
    raw = _readraw(input, format, use_mmap, skipstart, comments, comment_char)

    rows, cols = size(raw)
    @assert cols == length(types) "Number of cols in file is not $(length(types)): found $cols"

    # first row contains header
    header = [ Symbol(raw[1, col]) for col in 1:cols ]
    schema = Schema(header, types)
    table = Table(schema)

    return _read_data!(table, raw, format; header=true)
end

# Converts to string
function _write_string(io::IO, value::T, format::CSVFormat) where {T<:AbstractFloat}
    local result::String
    result = tostring(value)

    if format.decimal_separator != '.'
        result = replace(result, '.' => ',')
    end

    # TODO : support thousands_separator
    write(io, result)
end

_write_string(io::IO, value::T, format::CSVFormat) where {T<:Integer} = write(io, value)

function _write_string(io::IO, value::T, format::CSVFormat) where {T<:AbstractString}
    # Apply quotes if there´s a delimiter inside the string (replicate MS Excel behavior)
    if in(format.dlm, value)
        write(io, '"')
        write(io, value)
        write(io, '"')
    else
        write(io, value)
    end
end

_write_string(io::IO, value::Int, format::CSVFormat) = write(io, string(value))
_write_string(io::IO, value::T, format::CSVFormat) where {T<:Dates.TimeType} = write(io, Dates.format(value, format.date_format))
_write_string(io::IO, ::Missing, format::CSVFormat) = write(io, format.missing_str)

# Fallback
function _write_string(io::IO, value, format::CSVFormat)
    x = string(value)
    # Apply quotes if there´s a delimiter inside the string (replicate MS Excel behavior)
    if in(format.dlm, x)
        write(io, '"')
        write(io, x)
        write(io, '"')
    else
        write(io, x)
    end
end

function writecsv(filepath::String, tb::Union{AbstractDataFrame, Table}, format::CSVFormat=CSVFormat(); header::Bool=true)

    LB = '\n' # line break
    io = open(filepath, "w")
    rows, cols = size(tb)

    try
        # writes header to file
        if header
            h = names(tb)
            for i in eachindex(h)
                write(io, h[i])
                i != cols && write(io, format.dlm)
            end
            write(io, LB)
        end

        # data
        for r in 1:rows
            for c in 1:cols
                _write_string(io, tb[r,c], format)
                c != cols && write(io, format.dlm)
            end
            write(io, LB)
        end

    finally
        close(io)
    end

    nothing
end

# TODO: functions to append to existing Table
#=
function readcsv!(filepath::String, tb::Table;
    dlm::Char=';', null_str::String="", decimal_separator::Char=',', thousands_separator::Nullable{Char}=Nullable{Char}(), header=true)

end
=#

#function _read_column!(col_data::Vector{T}, raw_column::Vector{String}) where {T}

#end

#function _read_value!(col_data::NullableVector{T}, row_index::Int, value::String) where {T}

#end
