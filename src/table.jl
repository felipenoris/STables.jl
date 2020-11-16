
function Base.isequal(t1::Table, t2::Table)
    return (t1.schema == t2.schema) && isequal(t1.data, t2.data)
end

# shallow-copy
function Base.copy(t::Table)
    new_schema = copy(t.schema)
    new_data = copy(t.data)
    return Table(new_schema, new_data)
end

function Base.deepcopy(t::Table)
    new_schema = deepcopy(t.schema)
    new_data = deepcopy(t.data)
    return Table(new_schema, new_data)
end

function Table(schema::Schema)
    cols = ncol(schema)
    data = Vector{Any}(undef, cols)
    @inbounds for c in 1:cols
        col_type = schema.types[c]
        col_data = _create_table_column(col_type, 0)
        data[c] = col_data
    end
    return Table(schema, data)
end

# DataFrame like creation
# Table(a=[1,2], b=[3,missing])
function Table(; kwargs...)
    n = length(kwargs)

    column_names = Vector{Symbol}(undef, n)
    column_types = Vector{Type}(undef, n)
    data = Vector{Any}(undef, n)

    for (i, p) in enumerate(kwargs)
        name, column_data = p
        @assert isa(column_data, Vector) "$(i)th argument is not a vector: $column_data."
        column_names[i] = name
        column_types[i] = eltype(column_data)
        data[i] = column_data
    end

    return Table(Schema(column_names, column_types), data)
end

function Table(schema::Schema, matrix::Array{T, 2}) where {T}
    rows, cols = size(matrix)
    tb = Table(schema, rows)
    for c in 1:cols
        for r in 1:rows
            tb[r,c] = matrix[r,c]
        end
    end
    return tb
end

_create_table_column(::Type{Union{Missing, T}}, rows::Int) where {T} = Vector{Union{Missing, T}}(missing, rows)
_create_table_column(::Type{String}, rows::Int) = fill("", rows)
_create_table_column(::Type{T}, rows::Int) where {T<:Number} = zeros(T, rows)
_create_table_column(::Type{Date}, rows::Int) = fill(Date(0), rows)
_create_table_column(::Type{T}, rows::Int) where {T} = error("Method not implemented for type $T")

# Creates a table with number of rows = rows.
# Table data is not initialized.
function Table(schema::Schema, rows::Int)
    cols = length(names(schema))
    data = Vector{Any}(undef, cols)
    @inbounds for c in 1:cols
        col_type = schema.types[c]
        col_data = _create_table_column(col_type, rows)
        data[c] = col_data
    end
    Table(schema, data)
end

function Table(schema::Schema, df::DataFrame)
    rows, cols = size(df)
    @assert cols == length(schema.names) "number of columns in DataFrame ($cols) does not match schema ($(length(schema.names)))."
    data = Vector{Any}(undef, cols)

    @inbounds for (c, df_column) in enumerate(DataFrames.eachcol(df))
        @assert eltype(df_column) == schema.types[c] "Type mismatch between schema ($(schema.types[c])) and DataFrame ($(eltype(df_column))) for column $c."
        data[c] = copy(df_column)
    end

    Table(schema, data)
end

function Table(types::Vector{T}, df::DataFrame) where {T<:Type}
    head = names(df)
    schema = Schema(head, types)
    return Table(schema, df)
end

function Table(df::DataFrame)
    rows, cols = size(df)
    col_names = copy(names(df))
    col_types = Vector{Type}(undef, cols)

    data = Vector{Any}(undef, cols)

    @inbounds for (c, df_column) in enumerate(DataFrames.eachcol(df))
        col_types[c] = eltype(df_column)
        data[c] = copy(df_column)
    end

    return Table(Schema(col_names, col_types), data)
end

DataFrames.names(t::Table) = names(t.schema)

function DataFrames.rename!(tb::Table, new_header::Vector)
    new_header_as_sym = new_header_as_sym = [ Symbol(x) for x in new_header]
    DataFrames.rename!(tb.schema, new_header_as_sym)
end

"""
    column_index(tb::Table, colname::Symbol)

Returns the index of the column `colname`.
Returns `nothing` if the column is not found.
"""
column_index(tb::Table, colname::Symbol) = findfirst(c->c==colname, names(tb))

function Base.getindex(tb::Table, colname::Symbol)
    index = column_index(tb, colname)
    @assert index != nothing "Couldn´t find column $colname"
    return tb.data[index]
end

Base.getindex(tb::Table, r::Int, c::Int) = tb.data[c][r]
Base.getindex(tb::Table, colname::AbstractString) = getindex(tb, Symbol(colname))
Base.getindex(tb::Table, ::Colon, c::Int) = tb.data[c]
Base.getindex(tb::Table, r::Int, ::Colon) = error("Not supported. Try Tables.TableRow(tb, $r)[:]")
Base.getindex(tb::Table, r::Int, colname::Symbol) = getindex(tb, colname)[r]
Base.getindex(tb::Table, colname::Symbol, r::Int) = getindex(tb, r, colname)

"""
    _column_index__create_column(tb::Table, column_description::Pair{Symbol, DataType})

Returns the index of the column `colname`.
If the column is not found, creates a new column and returns its index.
"""
function _column_index__create_column(tb::Table, column_description::Pair{Symbol, T}) where {T<:Type}
    colname = column_description[1]
    coltype = column_description[2]

    index = column_index(tb, colname)

    # Creates a new column if couldn´t find it
    if index == nothing
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

function Base.setindex!(tb::Table, value::Vector{T}, colname::Symbol) where {T}
    index = _column_index__create_column(tb, colname => T)
    r = nrow(tb)
    @assert length(value) == r
    @assert T == tb.schema.types[index]
    tb.data[index] = value
end

function Base.setindex!(tb::Table, value, colname::Symbol)
    index = _column_index__create_column(tb, colname => typeof(value))
    fill!(tb.data[index], value)
end

Base.isempty(tb::Table) = isempty(tb.data)

function Base.size(tb::Table)
    isempty(tb) && return (0, 0)
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

function Base.show(io::IO, table::Table)
    r, c = size(table)
    println(io, "Table [$r, $c]")
end

function Base.append!(tb::Table, rows::Table)
    @assert tb.schema == rows.schema "Schemas don't match"
    for c in 1:ncol(tb)
        append!(tb.data[c], rows.data[c])
    end
    return tb
end

# append row to table
function Base.push!(tb::Table, row::Array{T,1}) where {T}
    # Check number of columns
    cols = length(row)
    @assert cols == ncol(tb) "Number of columns doesn't match vector size."

    tb_tmp = Table(tb.schema, 1)

    # Set values
    for i = 1:cols
        tb_tmp[1, i] = row[i]
    end
    return append!(tb, tb_tmp)
end

Base.append!(tb::Table, row::Array{T,1}) where {T} = push!(tb, row)

function Base.append!(tb::Table, data::Array{T,2}) where {T}
    # Check number of columns
    cols = ncol(tb)
    data_rows, data_cols = size(data)
    @assert cols == data_cols "Number of columns doesn't match"

    tb_tmp = Table(tb.schema, data)
    return append!(tb, tb_tmp)
end

function Base.vcat(tb1::Table, tb2::Table)
    @assert tb1.schema == tb2.schema "Schemas don't match"
    tb1_copy = deepcopy(tb1)
    tb2_copy = deepcopy(tb2)
    return append!(tb1_copy, tb2_copy)
end

function Base.vcat(tb::Table, data::Array{T,1}) where {T}
    if isempty(data)
        return deepcopy(tb)
    else
        tb_copy = deepcopy(tb)
        return push!(tb_copy, data)
    end
end

function Base.vcat(tb::Table, data::Array{T,2}) where {T}
    if isempty(data)
        return deepcopy(tb)
    else
        tb_copy = deepcopy(tb)
        return append!(tb_copy, data)
    end
end
