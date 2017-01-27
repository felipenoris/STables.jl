
using Formatting

function _read_column{T<:AbstractString}(raw_column::Vector{String}, ::Type{T}, ROW_OFFSET::Int, FST_DATAROW_INDEX::Int, rows::Int;
    null_str::String="", decimal_separator::Char=',', thousands_separator::Nullable{Char}=Nullable{Char}())

    col_data = _create_table_column(T, rows + ROW_OFFSET)
    for r in FST_DATAROW_INDEX:rows
        r_ = r + ROW_OFFSET # r_ is the line index of the destination table. If raw contains a header, r_ = r - 1 . Otherwise, r_ = r
        @inbounds col_data[r_] = raw_column[r]
    end
    return col_data
end

function _read_column{T<:Number}(raw_column::Vector{String}, ::Type{T}, ROW_OFFSET::Int, FST_DATAROW_INDEX::Int, rows::Int;
    null_str::String="", decimal_separator::Char=',', thousands_separator::Nullable{Char}=Nullable{Char}())

    col_data = _create_table_column(T, rows + ROW_OFFSET)

    if !isnull(thousands_separator)
        ts_char = get(thousands_separator)
        ts_char == decimal_separator && error("Thousands and decimals separators ($ts_char) must be different")
    end

    for r in FST_DATAROW_INDEX:rows
        r_ = r + ROW_OFFSET # r_ is the line index of the destination table. If raw contains a header, r_ = r - 1 . Otherwise, r_ = r
        @inbounds value = raw_column[r]
        
        # Converts 100.000.000,00 to 100000000,00
        if !isnull(thousands_separator)
            value = replace(value, ts_char, "")
        end

        # Converts 1000,00 to 1000.00
        if decimal_separator != '.'
            value = replace(value, decimal_separator, '.')
        end

        @inbounds col_data[r_] = parse(T, value)
    end
    return col_data
end

_read_column{T}(raw_column::Vector{String}, ::Type{T}, ROW_OFFSET::Int, FST_DATAROW_INDEX::Int, rows::Int;
    null_str::String="", decimal_separator::Char=',', thousands_separator::Nullable{Char}=Nullable{Char}()) = error("Parsing $T not implemented.")

function _read_column{T<:AbstractString}(raw_column::Vector{String}, ::Type{Nullable{T}}, ROW_OFFSET::Int, FST_DATAROW_INDEX::Int, rows::Int;
    null_str::String="", decimal_separator::Char=',', thousands_separator::Nullable{Char}=Nullable{Char}())

    col_data = _create_table_column(Nullable{T}, rows + ROW_OFFSET)
    for r in FST_DATAROW_INDEX:rows
        r_ = r + ROW_OFFSET # r_ is the line index of the destination table. If raw contains a header, r_ = r - 1 . Otherwise, r_ = r
        
        @inbounds value = raw_column[r]

        if value == null_str
            continue
        else
            @inbounds col_data[r_] = value
        end
    end
    return col_data
end

function _read_column{T<:Number}(raw_column::Vector{String}, ::Type{Nullable{T}}, ROW_OFFSET::Int, FST_DATAROW_INDEX::Int, rows::Int;
    null_str::String="", decimal_separator::Char=',', thousands_separator::Nullable{Char}=Nullable{Char}())

    col_data = _create_table_column(Nullable{T}, rows + ROW_OFFSET)

    if !isnull(thousands_separator)
        ts_char = get(thousands_separator)
        ts_char == decimal_separator && error("Thousands and decimals separators ($ts_char) must be different")
    end

    for r in FST_DATAROW_INDEX:rows
        r_ = r + ROW_OFFSET # r_ is the line index of the destination table. If raw contains a header, r_ = r - 1 . Otherwise, r_ = r
        @inbounds value = raw_column[r]
        
        if value == null_str
            continue
        else
            # Converts 100.000.000,00 to 100000000,00
            if !isnull(thousands_separator)
                value = replace(value, ts_char, "")
            end

            # Converts 1000,00 to 1000.00
            if decimal_separator != '.'
                value = replace(value, decimal_separator, '.')
            end
            @inbounds col_data[r_] = parse(T, value)
        end
    end
    return col_data
end

_read_column{T}(raw_column::Vector{String}, ::Type{Nullable{T}}, ROW_OFFSET::Int, FST_DATAROW_INDEX::Int, rows::Int;
    null_str::String="", decimal_separator::Char=',', thousands_separator::Nullable{Char}=Nullable{Char}()) = error("Parsing Nullable{$T} not implemented.")

function _read_data!(table::Table, raw::Array{String,2};
    dlm::Char=';', null_str::String="", decimal_separator::Char=',', thousands_separator::Nullable{Char}=Nullable{Char}(), header::Bool=true)
    
    rows, cols = size(raw)
    
    # Check if header is consistent with schema
    cols != length(table.schema.names) && error("CSV not consistent with given schema. ncols in file: $cols; ncols in schema: $(length(schema.names)).")
    
    if header
        ROW_OFFSET = -1
    else
        ROW_OFFSET = 0
    end

    FST_DATAROW_INDEX = 1 - ROW_OFFSET # this is the index of the first data row in $raw

    col_array = Array{Any}(cols)
    
    for col in 1:cols
        @inbounds col_type = table.schema.types[col]
        @inbounds col_array[col] = _read_column(raw[:,col], col_type, ROW_OFFSET, FST_DATAROW_INDEX, rows; null_str=null_str, decimal_separator=decimal_separator, thousands_separator=thousands_separator)
    end
    
    # Set table data without checks
    table.data = col_array
    return table
end

"""
    readcsv(input, schema::Schema; dlm, null_str, decimal_separator, thousands_separator, header, use_mmap)

header :: Bool Tells if the input file has a header in the first line. Default is `true`.
"""
function readcsv(input, schema::Schema;
    dlm::Char=';', null_str::String="", decimal_separator::Char=',', thousands_separator::Nullable{Char}=Nullable{Char}(), header::Bool=true, use_mmap::Bool=false )
    
    # Checks if input is consistent
    if !isnull(thousands_separator)
        ts_char = get(thousands_separator)
        if ts_char == decimal_separator
            error("decimal_separator ($decimal_separator) conflicts with thousands_separator ($ts_char).")
        end
    end

    raw = readdlm(input, dlm, String; use_mmap=use_mmap)

    tb = Table(schema)
    return _read_data!(tb, raw; dlm=dlm, null_str=null_str, decimal_separator=decimal_separator, thousands_separator=thousands_separator, header=header)
end

function readcsv(input, types::Vector{DataType};
    dlm::Char=';', null_str::String="", decimal_separator::Char=',', thousands_separator::Nullable{Char}=Nullable{Char}(), use_mmap::Bool=false )
    
    # Checks if input is consistent
    if !isnull(thousands_separator)
        ts_char = get(thousands_separator)
        if ts_char == decimal_separator
            error("decimal_separator ($decimal_separator) conflicts with thousands_separator ($ts_char).")
        end
    end

    raw = readdlm(input, dlm, String; use_mmap=use_mmap)

    rows, cols = size(raw)
    cols != length(types) && error("Number of cols in file is not $(length(types)): found $cols")

    # first row contains header
    header = Vector{Symbol}(cols)
    for col in 1:cols
        header[col] = Symbol(raw[1, col])
    end
    schema = Schema(header, types)
    table = Table(schema)

    return _read_data!(table, raw; dlm=dlm, null_str=null_str, decimal_separator=decimal_separator, thousands_separator=thousands_separator, header=true)
end

# Converts to string
function _write_string{T<:AbstractFloat}(io::IO, value::T, dlm::Char, decimal_separator::Char, null_str::String, float_format::FormatSpec)
    local result::String
    result = fmt(float_format, value)

    if decimal_separator != '.'
        result = replace(result, '.', ',')
    end

    write(io, result)
end

_write_string{T<:Integer}(io::IO, value::T, dlm::Char, decimal_separator::Char, null_str::String, float_format::FormatSpec) = write(io, value)

function _write_string(io::IO, value::String, dlm::Char, decimal_separator::Char, null_str::String, float_format::FormatSpec)
    # Apply quotes if there´s a delimiter inside the string (replicate MS Excel behavior)
    if in(dlm, value)
        write(io, '"')
        write(io, value)
        write(io, '"')
    else
        write(io, value)
    end
end

_write_string(io::IO, value::Int, dlm::Char, decimal_separator::Char, null_str::String, float_format::FormatSpec) = write(io, string(value))

function _write_string{T}(io::IO, value::Nullable{T}, dlm::Char, decimal_separator::Char, null_str::String, float_format::FormatSpec)
    if isnull(value)
        write(io, null_str)
    else
        _write_string(io, lift(value), dlm, decimal_separator, null_str, float_format)
    end
end

# Fallback
function _write_string(io::IO, value, dlm::Char, decimal_separator::Char, null_str::String, float_format::FormatSpec)
    x = string(value)
    # Apply quotes if there´s a delimiter inside the string (replicate MS Excel behavior)
    if in(dlm, x)
        write(io, '"')
        write(io, x)
        write(io, '"')
    else
        write(io, x)
    end
end

# Default behavior is to consider ';' as delimiter and ',' as a decimal separator.
function writecsv(filepath::String, tb::Union{AbstractDataFrame, Table} ;
    dlm::Char=';', null_str::String="", decimal_separator::Char=',', header::Bool=true, float_format::FormatSpec=FormatSpec(".15f") )
    const LB = '\n' # line break
    io = open(filepath, "w")
    rows, cols = size(tb)

    try
        # writes header to file
        if header
            h = names(tb)
            for i in eachindex(h)
                write(io, h[i])
                i != cols && write(io, dlm)
            end
            write(io, LB)
        end
            
        # data
        for r in 1:rows
            for c in 1:cols
                _write_string(io, tb[r,c], dlm, decimal_separator, null_str, float_format)
                c != cols && write(io, dlm)
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

#function _read_column!{T}(col_data::Vector{T}, raw_column::Vector{String})

#end

#function _read_value!{T}(col_data::NullableVector{T}, row_index::Int, value::String)

#end
