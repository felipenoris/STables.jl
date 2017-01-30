
__precompile__(true)
module Tables

using NullableArrays
using DataFrames
using Lifting

include("types.jl")

function Base.:(==)(sa::Schema, sb::Schema)
    return (sa.names == sb.names) && (sa.types == sb.types)
end

function Base.isequal(t1::Table, t2::Table)
    return (t1.schema == t2.schema) && isequal(t1.data, t2.data)
end

function Base.copy(s::Schema)
    new_header = copy(s.names)
    new_types = copy(s.types)
    return Schema(new_header, new_types)
end

function Base.copy(t::Table)
    new_schema = copy(t.schema)
    new_data = copy(t.data)
    return Table(new_schema, new_data)
end

function Schema(header::Vector, types::Vector{DataType})
    header = [Symbol(x) for x in header]
    return Schema(header, types)
end

# Allow for syntax: Schema( [:a => String, :b => Int] )
function Schema(v::Vector{Pair{Symbol, DataType}})
    n = length(v)
    head = Vector{Symbol}(n)
    types = Vector{DataType}(n)

    for i in 1:n
        head[i] = v[i][1]
        types[i] = v[i][2]
    end

    return Schema(head, types)
end

function Table(schema::Schema)
    cols = ncol(schema)
    data = Vector{Any}(cols)
    for c in 1:cols
        col_type = schema.types[c]
        col_data = _create_table_column(col_type, 0)
        data[c] = col_data
    end
    return Table(schema, data)
end

function Table{T}(schema::Schema, matrix::Array{T, 2})
    rows, cols = size(matrix)
    tb = Table(schema, rows)
    for c in 1:cols
        for r in 1:rows
            tb[r,c] = matrix[r,c]
        end
    end
    return tb
end

_create_table_column{T}(::Type{Nullable{T}}, rows::Int) = NullableArray{T}(rows)
_create_table_column(::Type{String}, rows::Int) = fill("", rows)
_create_table_column{T<:Number}(::Type{T}, rows::Int) = zeros(T, rows)
_create_table_column(::Type{Date}, rows::Int) = fill(Date(0), rows)
_create_table_column{T}(::Type{T}, rows::Int) = error("Method not implemented for type $T")

# Creates a table with number of rows = rows.
# Table data is not initialized.
function Table(schema::Schema, rows::Int)
    cols = length(names(schema))
    data = Vector{Any}(cols)
    for c in 1:cols
        col_type = schema.types[c]
        col_data = _create_table_column(col_type, rows)
        data[c] = col_data
    end
    Table(schema, data)
end

function _has_only_nullables(types::Vector{DataType}) :: Bool
    for t in types
        if !(t <: Nullable)
            return false
        end
    end
    return true
end

function Table(schema::Schema, df::DataFrame)
    
    @assert _has_only_nullables(schema.types) "Cannot create Table from DataFrame with non-Nullable types in Schema."

    rows, cols = size(df)
    @assert cols == length(schema.names) "number of columns in DataFrame ($cols) does not match schema ($( length(schema.names) ))."
    data = Vector{Any}(cols)
    for c in 1:cols
        col_type = schema.types[c]
        col_data = _create_table_column(col_type, rows)
        
        for r in 1:rows
            value = df[r,c]
            if isna(value)
                col_data[r] = col_type()
            else
                col_data[r] = value
            end
        end
        data[c] = col_data
    end

    Table(schema, data)
end

function Table(types::Vector{DataType}, df::DataFrame)
    head = names(df)
    schema = Schema(head, types)
    return Table(schema, df)
end

# Infer types, using NullableArrays everywhere
function Table(df::DataFrame)
    rows, cols = size(df)
    types = Vector{DataType}(cols)

    for c in 1:cols
        types[c] = Nullable{Any}
        for r in 1:rows
            value = df[r,c]
            
            if !isna(value)
                types[c] = unlift(typeof(value))
                break
            end
        end
    end
    return Table(types, df)
end

DataFrames.names(t::Table) = names(t.schema)

function Base.push!(s::Schema, column_description::Pair{Symbol, DataType})
    push!(s.names, column_description[1])
    push!(s.types, column_description[2])
end

DataFrames.names(s::Schema) = s.names

function DataFrames.names!(s::Schema, new_names::Vector{Symbol})
    s.names = new_names
end

function DataFrames.names!(tb::Table, new_header::Vector)
    new_header_as_sym = new_header_as_sym = [ Symbol(x) for x in new_header]
    names!(tb.schema, new_header_as_sym)
end

"""
    column_index(tb::Table, colname::Symbol)

Returns the index of the column `colname`.
Returns 0 if the column is not found.
"""
column_index(tb::Table, colname::Symbol) = findfirst(names(tb), colname)

function Base.getindex(tb::Table, colname::Symbol)
    index = column_index(tb, colname)
    @assert index != 0 "Couldn´t find column $colname"
    tb.data[index]
end

Base.getindex(tb::Table, r::Int, c::Int) = tb.data[c][r]
Base.getindex(tb::Table, colname::AbstractString) = getindex(tb, Symbol(colname))
Base.getindex(tb::Table, ::Colon, c::Int) = tb.data[c]
Base.getindex(tb::Table, r::Int, ::Colon) = error("Sorry... Not supported.")
Base.getindex(tb::Table, r::Int, colname::Symbol) = getindex(tb, colname)[r]
Base.getindex(tb::Table, colname::Symbol, r::Int) = getindex(tb, r, colname)

"""
    _column_index__create_column(tb::Table, column_description::Pair{Symbol, DataType})

Returns the index of the column `colname`.
If the column is not found, creates a new column and returns its index.
"""
function _column_index__create_column(tb::Table, column_description::Pair{Symbol, DataType})
    colname = column_description[1]
    coltype = column_description[2]

    index = column_index(tb, colname)

    # Creates a new column if couldn´t find it
    if index == 0
        r, c = size(tb)
        index = c + 1
        push!(tb.schema, column_description)
        col_data = _create_table_column(coltype, r)
        push!(tb.data, col_data)
    end
    return index
end

function Base.setindex!(tb::Table, value, r::Int, c::Int)
    tb.data[c][r] = value
end

function Base.setindex!(tb::Table, value, colname::Symbol, r::Int)
    index = _column_index__create_column(tb, colname => typeof(value))
    tb.data[index][r] = value
end

function Base.setindex!{T}(tb::Table, value::Vector{T}, colname::Symbol)
    index = _column_index__create_column(tb, colname => eltype(value))
    r = nrow(tb)
    @assert length(value) == r
    @assert eltype(value) == tb.schema.types[index]
    tb.data[index] = value
end

function Base.setindex!{T}(tb::Table, value::NullableVector{T}, colname::Symbol)
    index = _column_index__create_column(tb, colname => eltype(value))
    r = nrow(tb)
    @assert length(value) == r
    @assert eltype(value) == tb.schema.types[index]
    tb.data[index] = value
end

function Base.setindex!(tb::Table, value, colname::Symbol)
    index = _column_index__create_column(tb, colname => typeof(value))
    fill!(tb.data[index], value)
end

function Base.size(tb::Table)
    isempty(tb.data) && return 0, 0
    length(tb.data[1]), length(names(tb))
end

function DataFrames.nrow(tb::Table)
    r, c = size(tb)
    return r
end

function DataFrames.ncol(tb::Table)
    r, c = size(tb)
    return c
end

DataFrames.ncol(s::Schema) = length(s.names)

function Base.show(io::IO, table::Table)
    r, c = size(table)
    println(io, "Table [$r, $c]")
end

Base.showall(io::IO, table::Table) = show(io, table)

Base.getindex(tr::TableRow, c::Int) = getindex(tr.table, tr.row, c)
Base.getindex(tr::TableRow, column_name::Symbol) = getindex(tr.table, tr.row, column_name)
Base.setindex!(tr::TableRow, value, c::Int) = setindex!(tr.table, value, tr.row, c)
Base.setindex!(tr::TableRow, value, colname::Symbol) = setindex!(tr.table, value, colname, tr.row)
Base.length(tr::TableRow) = length(tr.table.data)

DataFrames.eachrow(tb::Table) = TableRowIterator(tb)
Base.start(itr::TableRowIterator) = 1
Base.done(itr::TableRowIterator, s) = s > length(itr.table.data[1])
Base.next(itr::TableRowIterator, s) = ( TableRow(itr.table, s), s+1 )

function Base.append!(tb::Table, rows::Table)
    @assert tb.schema == rows.schema "Schemas don't match"
    for c in 1:ncol(tb)
        append!(tb.data[c], rows.data[c])
    end
    return tb
end

function Base.push!{T}(tb::Table, row::Array{T,1})
    # Check number of columns
    const cols = length(row)
    @assert cols == ncol(tb) "Number of columns doesn't match vector size."

    row_lifted = Vector{Any}(cols)
    
    # Set values on row_lifted
    for i = 1:cols
        if isa(row[i], Nullable) && isnull(row[i])
            @assert tb.schema.types[i] <: Nullable "Column $i should be of a Nullable type ($(tb.schema.types[i]))"
            row_lifted[i] = tb.schema.types[i]()
        else
            row_lifted[i] = convert(tb.schema.types[i], lift(row[i]))
        end
    end

    tb_tmp = Table(tb.schema, 1)

    # Set values
    for i = 1:cols
        tb_tmp[1, i] = row_lifted[i]
    end
    return append!(tb, tb_tmp)
end

function Base.append!{T}(tb::Table, data::Array{T,2})
    # Check number of columns
    const cols = ncol(tb)
    data_rows, data_cols = size(data)
    @assert cols == data_cols "Number of columns doesn't match"

    data_lifted = Array(Any, data_rows, data_cols)
    for c in 1:data_cols
        for r in 1:data_rows
            if isa(data[r,c], Nullable) && isnull(data[r,c])
                @assert tb.schema.types[c] <: Nullable "Column $c should be of a Nullable type ($(tb.schema.types[c]))"
                data_lifted[r, c] = tb.schema.types[c]()
            else
                data_lifted[r,c] = convert(tb.schema.types[c], lift(data[r,c]))
            end
        end
    end

    tb_tmp = Table(tb.schema, data_lifted)
    return append!(tb, tb_tmp)
end

function Base.vcat(tb1::Table, tb2::Table)
    @assert tb1.schema == tb2.schema "Schemas don't match"
    tb1_copy = copy(tb1)
    tb2_copy = copy(tb2)
    return append!(tb1_copy, tb2_copy)
end

function Base.vcat{T}(tb::Table, data::Array{T,1})
    @assert !isempty(data) "Cannot vcat an empty data container"
    tb_copy = copy(tb)
    return push!(tb_copy, data)
end

function Base.vcat{T}(tb::Table, data::Array{T,2})
    @assert !isempty(data) "Cannot vcat an empty data container"
    tb_copy = copy(tb)
    return append!(tb_copy, data)
end

include("io.jl")

export Table, Schema, TableRow

end # module Tables
